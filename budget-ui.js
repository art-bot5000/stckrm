// ═══════════════════════════════════════════════════════════════════════════
//  BUDGET UI (lazy-loaded half, extracted from budget.js)
//
//  This file is LAZY-LOADED — it is NOT referenced by a <script> tag in
//  index.html. It is injected on demand by window._loadBudgetUI() (defined in
//  index.html, same once-only pattern as _loadScanner / _loadDemo) the first
//  time the user opens the Budget view. For the many sessions where a user
//  never opens Budget, ~7,400 lines stay out of the parse/execute path.
//
//  It depends on budget.js (core) having already loaded: it freely reads and
//  writes the core Budget state globals (bills, transactions, budgetCategories,
//  billInstances, budgetAccounts, incomeTemplates, incomeEntries,
//  budgetSettings, the *DeletedIds sets) and calls core functions (loadBudget,
//  the merge fns, createTransaction, getActiveBudgetCategories, _money, etc.)
//  by global name — exactly as it did when this was one file. Because both are
//  plain <script> (NOT ES modules) all top-level declarations remain global.
//
//  IMPORTANT — decorator ordering: this file contains the phase 3/4/4.5/5/5c
//  wrapper chain (const _origX = X; X = function(){ ... _origX ... }). These
//  run top-to-bottom at load time and MUST keep their original relative order.
//  The extraction preserved source order, so do not reorder functions here.
//
//  budget.js (core), budget-ui.js, app.js and index.html must all land in
//  GitHub together.
// ═══════════════════════════════════════════════════════════════════════════

function _getRolledMonth() {
  try { return localStorage.getItem(_LS_ROLLED_MONTH) || null; } catch (e) { return null; }
}
function _setRolledMonth(yyyymm) {
  try {
    if (yyyymm) localStorage.setItem(_LS_ROLLED_MONTH, yyyymm);
    else        localStorage.removeItem(_LS_ROLLED_MONTH);
  } catch (e) { /* private mode / quota — non-fatal */ }
}
function _getNewMonthDismissed() {
  try { return localStorage.getItem(_LS_NEWMONTH_DISMISS) || null; } catch (e) { return null; }
}
function _setNewMonthDismissed(yyyymm) {
  try {
    if (yyyymm) localStorage.setItem(_LS_NEWMONTH_DISMISS, yyyymm);
    else        localStorage.removeItem(_LS_NEWMONTH_DISMISS);
  } catch (e) { /* non-fatal */ }
}

// ── Date helpers ───────────────────────────────────────────────────────────
async function materialiseMonth(yyyymm, { force = false, persist = true } = {}) {
  const already = budgetSettings.materialisedMonths.includes(yyyymm);
  // Don't early-return on already-materialised. We still want to add
  // instances for newly-created templates that didn't exist when this
  // month was first materialised. The for-loop body is the idempotent
  // part — it skips templates whose instance already exists.

  const { year, month } = _parseYyyymm(yyyymm);
  const monthInstances  = (force ? {} : (billInstances[yyyymm] || {}));

  for (const tpl of bills) {
    if (tpl.archived) continue;
    if (!shouldBeDueInMonth(tpl, year, month)) continue;
    // Don't overwrite an existing instance unless forcing
    if (!force && monthInstances[tpl.id]) continue;
    const dom    = _clampDayOfMonth(tpl.dayOfMonth || 1, year, month);
    monthInstances[tpl.id] = {
      billId:         tpl.id,
      dueDate:        _isoDate(year, month, dom),
      expectedAmount: tpl.amount,
      actualAmount:   null,
      paidAt:         null,
      paidBy:         null,
      skipped:        false,
      source:         'manual',
      updatedAt:      _nowIso(),
    };
  }

  billInstances[yyyymm] = monthInstances;
  if (!already) budgetSettings.materialisedMonths.push(yyyymm);

  if (persist) {
    await saveBudgetLocal();
    _syncQueue?.enqueue('Generating bills…');
  }
  return monthInstances;
}

// Backfill paid saving instances for a split bill's current cycle. For each
// month strictly before the current calendar month within the active cycle
// (between previous payment and next payment), creates a saving instance
// marked `paidAt: nowIso`. Skips months that already have an instance for
// this bill. Used when a user enables split mode — we credit them with the
// money they've been setting aside on the bill's natural calendar.
//
// We deliberately DON'T auto-pay the current month — the user pays that
// instance themselves so they have control over when in the month it's
// recorded.
async function _backfillSavingInstancesForBill(template) {
  if (!template || template.paymentStrategy !== 'split') return 0;
  if (!template.splitInto || !template.splitInto.count) return 0;
  const today = new Date();
  const todayY = today.getFullYear();
  const todayM = today.getMonth();
  const todayYyyymm = _yyyymm(today);

  // Find the cycle's start: previous payment date or template createdAt,
  // whichever is later. From there we walk forward month by month.
  const prevDue = _prevDueDateForTemplate(template, todayYyyymm, /* respectCreatedAt */ false);
  const createdIso = (template.createdAt || _nowIso()).slice(0, 10);
  // If a real previous due exists, we anchor on it (split mode = "I save on
  // bill's calendar"). Else fall back to createdAt.
  const cycleStartIso = prevDue || createdIso;
  const cycleStart    = new Date(cycleStartIso + 'T12:00:00');

  let touched = 0;
  // Walk from the month AFTER cycleStart, up to (but not including) today's month.
  let cursorY = cycleStart.getFullYear();
  let cursorM = cycleStart.getMonth() + 1;
  if (cursorM > 11) { cursorM = 0; cursorY++; }
  let safety = 0;
  while (safety++ < 36) { // 3 years horizon
    if (cursorY > todayY || (cursorY === todayY && cursorM >= todayM)) break;
    if (_isSplitBillSavingMonth(template, cursorY, cursorM)) {
      const dom = _clampDayOfMonth(template.dayOfMonth || 1, cursorY, cursorM);
      const dueDate = _isoDate(cursorY, cursorM, dom);
      const yyyymm = `${cursorY}-${String(cursorM + 1).padStart(2, '0')}`;
      if (!billInstances[yyyymm]) billInstances[yyyymm] = {};
      // Same uniform key format as payment instances. The `kind` field on
      // the value distinguishes the instance type.
      const key = `${template.id}__${dueDate}`;
      if (!billInstances[yyyymm][key]) {
        const perPeriod = Math.round((template.amount / template.splitInto.count) * 100) / 100;
        billInstances[yyyymm][key] = {
          billId:         template.id,
          dueDate,
          expectedAmount: perPeriod,
          actualAmount:   perPeriod,
          paidAt:         _nowIso(),     // backfilled as already paid
          paidBy:         _kvEmailHash || null,
          skipped:        false,
          source:         'split-saving-backfill',
          kind:           'saving',
          updatedAt:      _nowIso(),
        };
        touched++;
        // Track the month as materialised so we don't re-walk later.
        if (!budgetSettings.materialisedMonths.includes(yyyymm)) {
          budgetSettings.materialisedMonths.push(yyyymm);
        }
      }
    }
    cursorM++;
    if (cursorM > 11) { cursorM = 0; cursorY++; }
  }

  if (touched > 0) {
    await saveBudgetLocal();
    _syncQueue?.enqueue();
  }
  return touched;
}

// Auto-roll unpaid saving instances from past months into "paid" so that
// when the user opens a new month, the previous month's saving (if not
// manually paid) is automatically credited to carry-over. The user keeps
// control during the current month — only strictly-past-month instances
// are rolled.
//
// Called from renderBudget so it runs lazily on every dashboard view.
// Cheap: skips instantly if there's nothing to roll.
async function _autoRollPastSavingInstances() {
  const today = new Date();
  const todayMonthStart = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, '0')}-01`;
  let touched = 0;
  for (const yyyymm of Object.keys(billInstances)) {
    // Only sweep months strictly before the current month
    if (yyyymm >= todayMonthStart.slice(0, 7)) continue;
    for (const key of Object.keys(billInstances[yyyymm])) {
      const inst = billInstances[yyyymm][key];
      if (inst.kind !== 'saving') continue;
      if (inst.paidAt) continue;
      if (inst.skipped) continue;
      // Mark as auto-rolled: paidAt set, source flagged so we can tell it
      // apart from manually-paid in the future if needed.
      inst.paidAt       = _nowIso();
      inst.actualAmount = inst.actualAmount ?? inst.expectedAmount;
      inst.source       = 'split-saving-autoroll';
      inst.updatedAt    = _nowIso();
      touched++;
    }
  }
  if (touched > 0) {
    await saveBudgetLocal();
    _syncQueue?.enqueue();
  }
  return touched;
}

// Self-healing: collapse duplicate saving instances for the same split bill
// within the same calendar month down to a single instance. A historical
// double-run of the saving-instance backfill (and an old key-format
// migration) left some months with two saving rows for one bill, which
// inflated the "X/N mo saved" count. A monthly-split bill can only ever have
// ONE saving instance per calendar month, so any extras are safe to drop.
//
// Dedup rule per (billId, yyyymm) group of kind:'saving' instances:
//   • Prefer to KEEP a paid, non-skipped instance (so we don't lose credit).
//   • Among equally-eligible, keep the earliest updatedAt (stable).
//   • Delete the rest.
// Cheap: only writes when a real duplicate is found.
async function _dedupeSavingInstances() {
  let removed = 0;
  for (const yyyymm of Object.keys(billInstances)) {
    const month = billInstances[yyyymm];
    const groups = {}; // billId -> [{ key, inst }]
    for (const key of Object.keys(month)) {
      const inst = month[key];
      if (!inst || inst.kind !== 'saving') continue;
      (groups[inst.billId] ||= []).push({ key, inst });
    }
    for (const billId of Object.keys(groups)) {
      const list = groups[billId];
      if (list.length <= 1) continue; // no duplicate
      list.sort((a, b) => {
        const aPaid = (a.inst.paidAt && !a.inst.skipped) ? 0 : 1;
        const bPaid = (b.inst.paidAt && !b.inst.skipped) ? 0 : 1;
        if (aPaid !== bPaid) return aPaid - bPaid;
        return (a.inst.updatedAt || '').localeCompare(b.inst.updatedAt || '');
      });
      for (let i = 1; i < list.length; i++) {
        delete month[list[i].key];
        removed++;
      }
    }
    if (Object.keys(month).length === 0) delete billInstances[yyyymm];
  }
  if (removed > 0) {
    await saveBudgetLocal();
    _syncQueue?.enqueue();
  }
  return removed;
}

// ── Bill template CRUD ─────────────────────────────────────────────────────
async function createBill(input) {
  const tpl = {
    id:             'bill_' + (typeof uid === 'function' ? uid() : Date.now() + '_' + Math.random().toString(36).slice(2, 7)),
    name:           (input.name || '').trim() || 'Untitled bill',
    amount:         Number(input.amount) || 0,
    variableAmount: !!input.variableAmount,
    frequency:      input.frequency || { unit: 'month', interval: 1, anchorMonth: null },
    dayOfMonth:     Math.max(1, Math.min(31, Number(input.dayOfMonth) || 1)),
    categoryId:     input.categoryId || null,
    notes:          input.notes || '',
    archived:       false,
    // Phase 5: payment strategy. 'lump' (default) = pay in full from that
    // month's income. 'split' = set aside a portion each period across the
    // cycle. splitInto only meaningful when paymentStrategy === 'split'.
    paymentStrategy: input.paymentStrategy === 'split' ? 'split' : 'lump',
    splitInto:       _normaliseSplitInto(input.splitInto, input.frequency),
    createdAt:      _nowIso(),
    updatedAt:      _nowIso(),
  };
  bills.push(tpl);
  await saveBudgetLocal();
  _syncQueue?.enqueue();
  return tpl;
}

// Coerce splitInto into a clean { unit, count } shape, or null.
// Only meaningful for split-strategy bills with a frequency longer than a
// single period of the chosen unit.
function _normaliseSplitInto(raw, freq) {
  if (!raw || typeof raw !== 'object') return null;
  const unit  = raw.unit === 'week' ? 'week' : 'month';
  const count = Math.max(1, Math.min(60, parseInt(raw.count, 10) || 1));
  return { unit, count };
}

async function updateBill(id, patch) {
  const idx = bills.findIndex(b => b.id === id);
  if (idx === -1) return null;
  bills[idx] = { ...bills[idx], ...patch, updatedAt: _nowIso() };
  await saveBudgetLocal();
  _syncQueue?.enqueue();
  return bills[idx];
}

async function archiveBill(id) {
  return updateBill(id, { archived: true });
}

// Hard delete: removes the template, all materialised instances across every
// month, AND records a tombstone so other devices syncing this account drop
// the bill rather than re-introducing it. Tombstones are append-only and
// cleaned up nightly server-side once propagated.
async function deleteBillHard(id) {
  bills = bills.filter(b => b.id !== id);
  // Also strip from all materialised months — handle both old (billId) and
  // new (`${billId}__${dueDate}`) key formats
  for (const yyyymm of Object.keys(billInstances)) {
    for (const key of Object.keys(billInstances[yyyymm])) {
      if (key === id || key.startsWith(`${id}__`)) {
        delete billInstances[yyyymm][key];
      }
    }
    if (Object.keys(billInstances[yyyymm]).length === 0) delete billInstances[yyyymm];
  }
  billsDeletedIds.add(id);
  await saveBudgetLocal();
  _syncQueue?.enqueue();
}

// ── Carry-over (split-strategy bills) ──────────────────────────────────────
// Bills with frequency longer than a single month can opt into a "split"
// strategy where the user sets aside a portion each month/week across the
// cycle. The total currently set aside (the "carry-over") is computed from:
//   amount per period       = template.amount / splitInto.count
//   periods elapsed in cycle = whole periods since the cycle's start anchor
//   accrued                 = min(periods elapsed, splitInto.count) × per-period
// The cycle's anchor is whichever is most recent of:
//   - the last paid instance's dueDate
//   - the bill's createdAt
// (We use the dueDate of the last payment, not paidAt, because the cycle is
//  conceptually anchored on the bill's calendar, not when it was actioned.)

// Returns a number (count of whole units between the two ISO dates, may be
// negative if to < from).
function _periodsBetween(fromIso, toIso, unit) {
  const a = new Date(fromIso + 'T12:00:00');
  const b = new Date(toIso   + 'T12:00:00');
  if (isNaN(a.getTime()) || isNaN(b.getTime())) return 0;
  if (unit === 'week') {
    return Math.floor((b.getTime() - a.getTime()) / (7 * MS_PER_DAY));
  }
  // 'month': calendar months between the two dates, ignoring day-of-month
  const months = (b.getFullYear() - a.getFullYear()) * 12 + (b.getMonth() - a.getMonth());
  return months;
}

// Returns the most recent paid-instance dueDate for a bill, or null if none.
function _lastPaidDueDate(billId) {
  let latest = null;
  for (const yyyymm of Object.keys(billInstances)) {
    for (const key of Object.keys(billInstances[yyyymm])) {
      const inst = billInstances[yyyymm][key];
      if (inst.billId !== billId || !inst.paidAt) continue;
      if (!latest || (inst.dueDate || '') > latest) latest = inst.dueDate || null;
    }
  }
  return latest;
}

// Returns true if the template is a split-strategy bill in an active
// saving cycle for the viewed month — i.e. NOT due this month, but the
// next-due date is in the future and we're between the previous payment
// (or template creation) and that next-due. Used to surface split bills
// in the "Saving up" section of the bills panel.
function _isSplitBillSavingForMonth(template, viewYyyymm) {
  if (!template || template.archived) return false;
  if (template.paymentStrategy !== 'split') return false;
  if (!template.splitInto || !template.splitInto.count) return false;
  const { year, month } = _parseYyyymm(viewYyyymm);
  // If due this month, it belongs in the regular bills list, not saving-up.
  if (shouldBeDueInMonth(template, year, month)) return false;
  // Need a future due date relative to the viewed month.
  const next = _nextDueDateForTemplate(template, viewYyyymm);
  if (!next) return false;
  // The viewed month must end on or after the bill's creation. Otherwise
  // we're looking at a month before the bill existed and there's nothing
  // to save up. (We compare end-of-month against createdAt date so a bill
  // created on the 3rd still surfaces in saving-up for that same month.)
  const created = (template.createdAt || _nowIso()).slice(0, 10);
  const endIso  = _isoDate(year, month, _daysInMonth(year, month));
  if (endIso < created) return false;
  return true;
}

// Stricter version of the above — returns true if (year, month) is a
// saving month for the template based purely on the bill's calendar
// (between previous and next payment dates), without the createdAt cap.
// Used by materialiseMonth when deciding whether to generate a saving
// instance, and by the backfill logic when seeding past saving months.
function getBillCycleProgressForMonth(template, viewYyyymm) {
  if (!template) return null;
  const split = template.splitInto;
  if (template.paymentStrategy !== 'split' || !split || !split.count) return null;
  const amount = Number(template.amount) || 0;
  if (amount <= 0) return null;

  // Cycle anchor: previous theoretical due date strictly before the viewed
  // month, ignoring the createdAt cap (the user picking split mode is
  // asserting they save on the bill's calendar). Falls back to createdAt
  // only if there's no prior due date in the bill's history at all.
  const prevDue = _prevDueDateForTemplate(template, viewYyyymm, /* respectCreatedAt */ false);
  const anchorIso = prevDue || (template.createdAt || _nowIso()).slice(0, 10);

  // End of viewed month
  const { year, month } = _parseYyyymm(viewYyyymm);
  const endIso = _isoDate(year, month, _daysInMonth(year, month));

  const elapsed = Math.max(0, _periodsBetween(anchorIso, endIso, split.unit));
  const slot    = Math.min(elapsed, split.count);
  const perPeriod = Math.round((amount / split.count) * 100) / 100;
  const accrued   = Math.round(perPeriod * slot * 100) / 100;

  return {
    accrued,
    target: amount,
    perPeriod,
    slot,
    totalSlots: split.count,
    cycleAnchorIso: anchorIso,
    nextDueIso:    _nextDueDateForTemplate(template, viewYyyymm),
    unit:          split.unit,
  };
}

// Returns the next-due (unpaid, not skipped) instance dueDate for a bill, or
// null. Used to know when the current cycle ends.
function _nextDueDate(billId) {
  const today = new Date().toISOString().slice(0, 10);
  let next = null;
  for (const yyyymm of Object.keys(billInstances)) {
    for (const key of Object.keys(billInstances[yyyymm])) {
      const inst = billInstances[yyyymm][key];
      if (inst.billId !== billId) continue;
      // Saving instances are paper-only set-asides, NOT the bill's payment.
      // The cycle is bounded by PAYMENT dates, so a saving instance must
      // never be mistaken for "next due" — that corrupts the carry-over
      // cycle window and the X/N saved count.
      if (inst.kind === 'saving') continue;
      if (inst.paidAt || inst.skipped) continue;
      if (!inst.dueDate) continue;
      if (inst.dueDate < today) continue; // past-due — not part of next cycle
      if (!next || inst.dueDate < next) next = inst.dueDate;
    }
  }
  return next;
}

// Returns the next month (yyyymm) the template is due on or after the given
// reference month. Walks forward from `fromYyyymm` (inclusive) up to a sane
// horizon. Useful for split bills where the next payment may be many
// months in the future and not yet materialised.
function getBillCarryOver(template, todayIso = null) {
  if (!template || template.archived) return null;
  if (template.paymentStrategy !== 'split') return null;
  const split = template.splitInto;
  if (!split || !split.count || split.count < 1) return null;
  const amount = Number(template.amount) || 0;
  if (amount <= 0) return null;

  const today = todayIso || new Date().toISOString().slice(0, 10);
  const todayYyyymm = today.slice(0, 7);
  const perPeriod = Math.round((amount / split.count) * 100) / 100;

  // Find the cycle's start. Priority:
  //   1. Most recent paid PAYMENT instance (real money out — wins).
  //   2. Else previous theoretical payment in the bill's calendar (for bills
  //      added mid-cycle, the user is asserting "I save on this calendar"
  //      so we anchor on the last theoretical payment).
  //   3. Else createdAt — fallback for brand-new bills with no prior cycle.
  const lastPaidPayment = _lastPaidPaymentDueDate(template.id);
  const prevTheoretical = _prevDueDateForTemplate(template, todayYyyymm, /* respectCreatedAt */ false);
  const createdIso      = (template.createdAt || _nowIso()).slice(0, 10);
  let cycleStartIso;
  if (lastPaidPayment && lastPaidPayment <= today) {
    cycleStartIso = lastPaidPayment;
  } else if (prevTheoretical && prevTheoretical <= today) {
    cycleStartIso = prevTheoretical;
  } else {
    cycleStartIso = createdIso;
  }

  // Find the next-due payment instance — that's where the current cycle ends.
  const nextDueIso = _nextDueDate(template.id)
                  || _nextDueDateForTemplate(template, todayYyyymm);

  // Sum paid SAVING instances strictly between cycleStartIso (exclusive)
  // and nextDueIso (exclusive — the payment month doesn't count toward
  // carry-over since paying that bill is what closes the cycle).
  //
  // SINGLE SOURCE OF TRUTH: count each distinct calendar MONTH once, never raw
  // instances. A monthly split bill saves at most once per month, so two
  // instances in the same month (a historical duplicate) must NOT count twice.
  // This keeps the header ("X of N months saved"), the dashboard row, and the
  // ticked month list in the timeline modal in perfect agreement, regardless
  // of any stray duplicate rows still sitting in the data.
  const countedMonths = new Set();
  let accrued = 0;
  for (const yyyymm of Object.keys(billInstances)) {
    for (const key of Object.keys(billInstances[yyyymm])) {
      const inst = billInstances[yyyymm][key];
      if (inst.billId !== template.id) continue;
      if (inst.kind !== 'saving') continue;
      if (!inst.paidAt) continue;
      if (inst.skipped) continue;
      if (!inst.dueDate) continue;
      if (cycleStartIso && inst.dueDate <= cycleStartIso) continue;
      if (nextDueIso && inst.dueDate >= nextDueIso) continue;
      const monthKey = inst.dueDate.slice(0, 7);
      if (countedMonths.has(monthKey)) continue; // dedupe by calendar month
      countedMonths.add(monthKey);
      accrued += (inst.actualAmount ?? inst.expectedAmount) || 0;
    }
  }
  const slot = countedMonths.size;
  accrued = Math.round(accrued * 100) / 100;

  // Is the bill due THIS calendar month and unpaid? If so, the bill itself
  // covers the full amount in the bills list — we exclude this bill's
  // carry-over from the dashboard total to avoid double-counting.
  let currentMonthIsPayment = false;
  if (nextDueIso && nextDueIso.slice(0, 7) === todayYyyymm) {
    // Look up the actual instance for this payment date
    const payInst = billInstances[todayYyyymm]?.[`${template.id}__${nextDueIso}`];
    if (payInst && !payInst.paidAt && !payInst.skipped) {
      currentMonthIsPayment = true;
    }
  }

  return {
    accrued,
    target: amount,
    perPeriod,
    slot,                       // count of paid saving instances in current cycle
    totalSlots: split.count,
    cycleAnchorIso: cycleStartIso,
    nextDueIso,
    unit:           split.unit,
    currentMonthIsPayment,
  };
}

// Returns the dueDate of the most recent paid PAYMENT instance for this
// bill. Used to anchor the start of the current saving cycle.
function _lastPaidPaymentDueDate(billId) {
  let latest = null;
  for (const yyyymm of Object.keys(billInstances)) {
    for (const key of Object.keys(billInstances[yyyymm])) {
      const inst = billInstances[yyyymm][key];
      if (inst.billId !== billId) continue;
      // Only payment instances start a new cycle. Saving instances don't.
      if (inst.kind === 'saving') continue;
      if (!inst.paidAt) continue;
      if (!latest || (inst.dueDate || '') > latest) latest = inst.dueDate || null;
    }
  }
  return latest;
}

// Sum of all active split bills' accrued amounts. Used by the dashboard tile.
// Bills currently in their payment month (where the bill itself is in the
// regular bills list as the full amount) are EXCLUDED from the total — the
// carry-over for those bills is conceptually "consumed" by the upcoming
// full payment, so counting it in the dashboard total would double-count.
// Bills with no accrued amount yet are excluded from the breakdown to keep
// the modal focused on bills that actually have money set aside.
function getTotalCarryOver() {
  let total = 0;
  const breakdown = [];
  for (const tpl of bills) {
    const co = getBillCarryOver(tpl);
    if (!co) continue;
    if (co.currentMonthIsPayment) continue;
    total += co.accrued;
    if (co.accrued > 0) breakdown.push({ template: tpl, ...co });
  }
  return { total: Math.round(total * 100) / 100, breakdown };
}

// Suggests a sensible default splitInto for a bill template based on its
// frequency. Quarterly → {month, 3}, six-monthly → {month, 6}, annual →
// {month, 12}, custom monthly N → {month, N}, custom weekly N → {week, N},
// custom yearly → {month, 12}. Monthly bills get null (no split applicable).
function _suggestSplitInto(freq) {
  if (!freq) return { unit: 'month', count: 3 };
  if (freq.unit === 'year')  return { unit: 'month', count: 12 * Math.max(1, freq.interval || 1) };
  if (freq.unit === 'month') {
    const n = Math.max(1, freq.interval || 1);
    if (n <= 1) return null;
    return { unit: 'month', count: n };
  }
  if (freq.unit === 'week') {
    const n = Math.max(1, freq.interval || 1);
    if (n <= 1) return null;
    return { unit: 'week', count: n };
  }
  return { unit: 'month', count: 3 };
}

// True if a bill template's frequency is long enough that a split strategy
// makes sense (i.e. > 1 month between payments). Used to decide whether to
// show the split UI in the editor.
function _billCanSplit(freq) {
  if (!freq) return false;
  if (freq.unit === 'year')  return true;
  if (freq.unit === 'month') return Math.max(1, freq.interval || 1) > 1;
  if (freq.unit === 'week')  return Math.max(1, freq.interval || 1) > 4; // > ~1 month
  return false;
}

// ── Bills import / template export ─────────────────────────────────────
// Lets the user bulk-add bills via CSV: download a template, fill it in,
// upload it, see a preview of what will be imported (with any per-row
// errors flagged), confirm to commit. The template carries inline comment
// lines (prefixed with #) explaining each field's accepted values.

function downloadBillsTemplate() {
  // Prepend a UTF-8 BOM so Excel opens the file in the right encoding.
  // Without it Excel mangles £, €, accented characters etc.
  const blob = new Blob(['\ufeff' + _BILLS_TEMPLATE_CSV], { type: 'text/csv;charset=utf-8' });
  const url  = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'stockroom-bills-template.csv';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  // Revoke shortly after to free the object URL — some browsers need a tick
  setTimeout(() => URL.revokeObjectURL(url), 1000);
  toast('Template downloaded — fill it in and use Import to bring the bills in');
}

// Minimal CSV parser supporting quoted fields and embedded commas / quotes.
// RFC 4180 style: quotes escape with double-quote, fields may be wrapped in
// quotes if they contain a comma, quote, or newline. Returns an array of
// row arrays (each row is an array of strings).
function _parseCsv(text) {
  // Strip BOM if present
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);
  const rows = [];
  let row = [];
  let field = '';
  let inQuotes = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') {
        if (text[i+1] === '"') { field += '"'; i++; } // escaped quote
        else inQuotes = false;
      } else {
        field += c;
      }
    } else {
      if (c === '"') {
        inQuotes = true;
      } else if (c === ',') {
        row.push(field); field = '';
      } else if (c === '\r') {
        // ignore — we'll handle the \n
      } else if (c === '\n') {
        row.push(field); field = '';
        rows.push(row); row = [];
      } else {
        field += c;
      }
    }
  }
  // Final field / row
  if (field.length || row.length) {
    row.push(field);
    rows.push(row);
  }
  return rows;
}

// Take parsed CSV rows and produce { items, errors } where items is an
// array of validated bill template inputs ready for createBill, and errors
// is an array of per-row issues for the preview modal.
function _validateBillsCsv(rows) {
  const items  = [];
  const errors = [];
  // Find the header row — first non-comment, non-empty row
  let headerIdx = -1;
  for (let i = 0; i < rows.length; i++) {
    const first = (rows[i][0] || '').trim();
    if (!first) continue;
    if (first.startsWith('#')) continue;
    headerIdx = i;
    break;
  }
  if (headerIdx === -1) {
    return { items: [], errors: [{ row: 0, msg: 'No header row found — make sure the file includes the column names line.' }] };
  }
  const header = rows[headerIdx].map(h => h.trim().toLowerCase());
  const col = (name) => header.indexOf(name);
  const required = ['name', 'amount', 'frequency', 'dayofmonth'];
  for (const r of required) {
    if (col(r) === -1) {
      return { items: [], errors: [{ row: headerIdx + 1, msg: `Required column "${r}" missing from header` }] };
    }
  }
  const iName    = col('name');
  const iAmount  = col('amount');
  const iVar     = col('variableamount');
  const iFreq    = col('frequency');
  const iDay     = col('dayofmonth');
  const iCat     = col('category');
  const iNotes   = col('notes');

  const validUnits = { monthly:'month', weekly:'week', yearly:'year', daily:'day', month:'month', week:'week', year:'year', day:'day' };
  const yesValues  = new Set(['yes', 'y', 'true', '1']);
  const noValues   = new Set(['no', 'n', 'false', '0', '']);

  // Build a category-name → id lookup so users can write the human name
  const catLookup = new Map();
  if (Array.isArray(budgetCategories)) {
    for (const c of budgetCategories) {
      if (c && c.name) catLookup.set(c.name.trim().toLowerCase(), c.id);
    }
  }

  for (let i = headerIdx + 1; i < rows.length; i++) {
    const row = rows[i];
    const first = (row[0] || '').trim();
    if (!first || first.startsWith('#')) continue; // skip blanks and comments
    const lineNum = i + 1; // 1-indexed for human display

    const name = (row[iName] || '').trim();
    if (!name) {
      errors.push({ row: lineNum, msg: 'Missing name — row skipped' });
      continue;
    }
    const amountRaw = (row[iAmount] || '').trim().replace(/[£$€¥,]|kr/g, '');
    const amount = parseFloat(amountRaw);
    if (!isFinite(amount) || amount < 0) {
      errors.push({ row: lineNum, msg: `"${name}": amount "${row[iAmount]}" is not a valid number — row skipped` });
      continue;
    }
    const freqRaw  = (iFreq >= 0 ? row[iFreq] : '').trim().toLowerCase();
    const freqUnit = validUnits[freqRaw];
    if (!freqUnit) {
      errors.push({ row: lineNum, msg: `"${name}": frequency "${row[iFreq]}" not recognised (use monthly/weekly/yearly/daily) — row skipped` });
      continue;
    }
    const dayRaw = (row[iDay] || '').trim();
    const day    = parseInt(dayRaw, 10);
    if (!isFinite(day) || day < 1 || day > 31) {
      errors.push({ row: lineNum, msg: `"${name}": dayOfMonth "${row[iDay]}" must be 1–31 — row skipped` });
      continue;
    }
    let variable = false;
    if (iVar >= 0) {
      const v = (row[iVar] || '').trim().toLowerCase();
      if (yesValues.has(v))      variable = true;
      else if (noValues.has(v))  variable = false;
      else {
        errors.push({ row: lineNum, msg: `"${name}": variableAmount "${row[iVar]}" should be yes/no — assumed no` });
        // don't skip; just default to no
      }
    }
    let categoryId = null;
    if (iCat >= 0) {
      const catRaw = (row[iCat] || '').trim();
      if (catRaw) {
        const found = catLookup.get(catRaw.toLowerCase());
        if (found) categoryId = found;
        else errors.push({ row: lineNum, msg: `"${name}": category "${catRaw}" not found — leaving uncategorised` });
      }
    }
    const notes = iNotes >= 0 ? (row[iNotes] || '').trim() : '';
    items.push({
      name, amount, variableAmount: variable,
      frequency: { unit: freqUnit, interval: 1, anchorMonth: null },
      dayOfMonth: day,
      categoryId, notes,
    });
  }
  return { items, errors };
}

// Open a file picker, parse the selected CSV, then show a preview modal.
function openImportBills() {
  if (!canWrite('budget')) { showLockBanner('budget'); return; }
  const input = document.createElement('input');
  input.type = 'file';
  input.accept = '.csv,text/csv,text/plain';
  input.style.display = 'none';
  input.addEventListener('change', async () => {
    const file = input.files && input.files[0];
    document.body.removeChild(input);
    if (!file) return;
    let text;
    try {
      text = await file.text();
    } catch (e) {
      toast('Could not read the file');
      return;
    }
    const rows = _parseCsv(text);
    const { items, errors } = _validateBillsCsv(rows);
    _showBillsImportPreview(items, errors, file.name);
  });
  document.body.appendChild(input);
  input.click();
}

function _showBillsImportPreview(items, errors, filename) {
  document.getElementById('bills-import-overlay')?.remove();
  const overlay = document.createElement('div');
  overlay.id = 'bills-import-overlay';
  overlay.style.cssText = 'position:fixed;inset:0;z-index:600;background:rgba(0,0,0,0.75);display:flex;align-items:center;justify-content:center;backdrop-filter:blur(4px);padding:16px';
  const close = () => overlay.remove();
  overlay.addEventListener('click', e => { if (e.target === overlay) close(); });

  const card = document.createElement('div');
  card.style.cssText = 'background:var(--surface);border-radius:14px;width:100%;max-width:640px;max-height:85vh;display:flex;flex-direction:column;box-shadow:0 12px 48px rgba(0,0,0,0.6)';
  // Header
  const header = document.createElement('div');
  header.style.cssText = 'padding:18px 20px 12px;border-bottom:1px solid var(--border)';
  const titleRow = document.createElement('div');
  titleRow.style.cssText = 'display:flex;justify-content:space-between;align-items:flex-start;gap:12px';
  const titleBox = document.createElement('div');
  const title = document.createElement('h3');
  title.style.cssText = 'font-size:17px;font-weight:700;margin:0';
  title.textContent = 'Import bills';
  const sub = document.createElement('p');
  sub.style.cssText = 'font-size:12px;color:var(--muted);margin:4px 0 0;font-family:var(--mono)';
  sub.textContent = filename;
  titleBox.append(title, sub);
  const xBtn = document.createElement('button');
  xBtn.style.cssText = 'background:transparent;border:0;color:var(--muted);font-size:24px;line-height:1;cursor:pointer;padding:0 4px';
  xBtn.innerHTML = '×';
  xBtn.title = 'Close';
  xBtn.addEventListener('click', close);
  titleRow.append(titleBox, xBtn);
  header.appendChild(titleRow);
  // Counts strip
  const countStrip = document.createElement('div');
  countStrip.style.cssText = 'display:flex;gap:14px;margin-top:10px;font-size:12px';
  const okCount = document.createElement('span');
  okCount.style.cssText = 'color:var(--ok);font-weight:700';
  okCount.textContent = `✓ ${items.length} ready to import`;
  countStrip.appendChild(okCount);
  if (errors.length) {
    const errCount = document.createElement('span');
    errCount.style.cssText = 'color:var(--warn);font-weight:700';
    errCount.textContent = `⚠ ${errors.length} issue${errors.length===1?'':'s'}`;
    countStrip.appendChild(errCount);
  }
  header.appendChild(countStrip);
  card.appendChild(header);

  // Body — scroll area with errors then preview
  const body = document.createElement('div');
  body.style.cssText = 'padding:14px 20px;overflow-y:auto;flex:1';

  if (errors.length) {
    const errBox = document.createElement('div');
    errBox.style.cssText = 'background:rgba(232,168,56,0.08);border:1px solid rgba(232,168,56,0.3);border-radius:8px;padding:10px 12px;margin-bottom:14px';
    const eh = document.createElement('div');
    eh.style.cssText = 'font-size:11px;font-weight:700;color:var(--warn);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:6px';
    eh.textContent = 'Issues found';
    errBox.appendChild(eh);
    for (const err of errors) {
      const row = document.createElement('div');
      row.style.cssText = 'font-size:12px;color:var(--text);margin:3px 0;font-family:var(--mono)';
      row.textContent = `Line ${err.row}: ${err.msg}`;
      errBox.appendChild(row);
    }
    body.appendChild(errBox);
  }

  if (items.length) {
    const ph = document.createElement('div');
    ph.style.cssText = 'font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px';
    ph.textContent = 'Preview';
    body.appendChild(ph);
    const list = document.createElement('div');
    list.style.cssText = 'display:flex;flex-direction:column;gap:6px';
    for (const item of items) {
      const r = document.createElement('div');
      r.style.cssText = 'display:flex;justify-content:space-between;align-items:center;gap:10px;background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:8px 12px';
      const left = document.createElement('div');
      left.style.minWidth = '0';
      const nm = document.createElement('div');
      nm.style.cssText = 'font-weight:600;font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap';
      nm.textContent = item.name;
      const meta = document.createElement('div');
      meta.style.cssText = 'font-size:11px;color:var(--muted);font-family:var(--mono);margin-top:2px';
      const freqLabel = ({ month:'monthly', week:'weekly', year:'yearly', day:'daily' })[item.frequency.unit] || item.frequency.unit;
      meta.textContent = `${freqLabel} · day ${item.dayOfMonth}${item.variableAmount ? ' · variable' : ''}${item.notes ? ' · ' + item.notes : ''}`;
      left.append(nm, meta);
      const amt = document.createElement('div');
      amt.style.cssText = 'font-weight:700;font-size:13px;color:var(--text);flex-shrink:0;font-family:var(--mono)';
      amt.textContent = _money(item.amount);
      r.append(left, amt);
      list.appendChild(r);
    }
    body.appendChild(list);
  } else {
    const empty = document.createElement('div');
    empty.style.cssText = 'text-align:center;padding:30px 0;color:var(--muted);font-size:13px';
    empty.textContent = 'No valid rows to import. Check the issues above and try again.';
    body.appendChild(empty);
  }
  card.appendChild(body);

  // Footer — actions
  const footer = document.createElement('div');
  footer.style.cssText = 'padding:12px 20px;border-top:1px solid var(--border);display:flex;justify-content:flex-end;gap:8px;flex-wrap:wrap';
  const cancelBtn = document.createElement('button');
  cancelBtn.className = 'btn btn-ghost';
  cancelBtn.textContent = 'Cancel';
  cancelBtn.addEventListener('click', close);
  footer.appendChild(cancelBtn);
  if (items.length) {
    const importBtn = document.createElement('button');
    importBtn.className = 'btn btn-primary';
    importBtn.textContent = `Import ${items.length} bill${items.length === 1 ? '' : 's'}`;
    importBtn.addEventListener('click', async () => {
      importBtn.disabled = true;
      cancelBtn.disabled = true;
      importBtn.textContent = 'Importing…';
      let added = 0;
      for (const item of items) {
        try { await createBill(item); added++; }
        catch (e) { console.warn('Bill create failed:', item.name, e); }
      }
      // Regenerate the current month so newly-imported bills materialise
      try { await budgetRegenerateMonth?.(); } catch (e) {}
      try { await renderBudget?.(); } catch (e) {}
      close();
      toast(`Imported ${added} bill${added === 1 ? '' : 's'}`);
    });
    footer.appendChild(importBtn);
  }
  card.appendChild(footer);
  overlay.appendChild(card);
  document.body.appendChild(overlay);
}

// ── Instance ops ───────────────────────────────────────────────────────────
// Phase 4: instances are keyed by `${billId}__${dueDate}` to support templates
// that produce multiple instances per month (weekly, 4-weekly). The (yyyymm,
// billId) signature is preserved for backwards compatibility — when there's
// exactly one instance per bill per month (the common case), no caller change
// is needed. When there's ambiguity, callers should pass dueDate explicitly.
// Find an instance by billId + dueDate. Looks at every key in the month
// because keys may have multiple formats: legacy `${billId}`, current
// `${billId}__${dueDate}`, or split-saving `${billId}__SAV__${dueDate}`.
// Matching by the instance's own fields rather than the key format avoids
// silent failures when key conventions change.
function _findInstanceKey(month, billId, dueDate) {
  if (!month) return null;
  // Fast path for the standard payment-instance key
  if (dueDate) {
    const standardKey = `${billId}__${dueDate}`;
    if (month[standardKey]) return standardKey;
  }
  // Legacy single-key format
  if (month[billId]) return billId;
  // Fall back to a scan — covers split-saving keys and any future variants.
  for (const key of Object.keys(month)) {
    const inst = month[key];
    if (!inst) continue;
    if (inst.billId !== billId) continue;
    if (dueDate && inst.dueDate !== dueDate) continue;
    return key;
  }
  return null;
}

function _getInstance(yyyymm, billId, dueDate = null) {
  const month = billInstances[yyyymm];
  if (!month) return null;
  const key = _findInstanceKey(month, billId, dueDate);
  return key ? month[key] : null;
}

async function _setInstance(yyyymm, billId, patch, dueDate = null) {
  if (!billInstances[yyyymm]) billInstances[yyyymm] = {};
  const month = billInstances[yyyymm];
  // Resolve to an existing key so we update in place. If nothing exists,
  // fall through to the legacy `billId` key (caller is creating fresh).
  let key = _findInstanceKey(month, billId, dueDate);
  if (!key) {
    // No existing instance — create one. Prefer the keyed-by-date format.
    key = dueDate ? `${billId}__${dueDate}` : billId;
  }
  const existing = month[key] || {};
  month[key] = {
    ...existing,
    ...patch,
    billId,
    updatedAt: _nowIso(),
  };
  await saveBudgetLocal();
  _syncQueue?.enqueue();
  return month[key];
}

async function markBillPaid(yyyymm, billId, { actualAmount = null, dueDate = null } = {}) {
  const patch = {
    paidAt:  _nowIso(),
    paidBy:  _kvEmailHash || null,
    skipped: false,
  };
  if (actualAmount !== null && actualAmount !== undefined) patch.actualAmount = Number(actualAmount);
  return _setInstance(yyyymm, billId, patch, dueDate);
}

async function markBillUnpaid(yyyymm, billId, dueDate = null) {
  return _setInstance(yyyymm, billId, { paidAt: null, paidBy: null }, dueDate);
}

async function skipBillInstance(yyyymm, billId, dueDate = null) {
  return _setInstance(yyyymm, billId, { skipped: true, paidAt: null }, dueDate);
}

async function unskipBillInstance(yyyymm, billId, dueDate = null) {
  return _setInstance(yyyymm, billId, { skipped: false }, dueDate);
}

async function setInstanceActualAmount(yyyymm, billId, amount, dueDate = null) {
  return _setInstance(yyyymm, billId, { actualAmount: Number(amount) }, dueDate);
}

// Regenerate a month from current templates (preserves paidAt for instances that still match)
async function regenerateMonth(yyyymm) {
  const { year, month } = _parseYyyymm(yyyymm);
  const old = billInstances[yyyymm] || {};
  const fresh = {};

  for (const tpl of bills) {
    if (tpl.archived) continue;
    if (!shouldBeDueInMonth(tpl, year, month)) continue;
    const dom = _clampDayOfMonth(tpl.dayOfMonth || 1, year, month);
    const existing = old[tpl.id];
    fresh[tpl.id] = {
      billId:         tpl.id,
      dueDate:        _isoDate(year, month, dom),
      expectedAmount: tpl.amount,
      actualAmount:   existing?.actualAmount ?? null,
      paidAt:         existing?.paidAt ?? null,
      paidBy:         existing?.paidBy ?? null,
      skipped:        existing?.skipped ?? false,
      source:         existing?.source ?? 'manual',
      updatedAt:      _nowIso(),
    };
  }

  billInstances[yyyymm] = fresh;
  if (!budgetSettings.materialisedMonths.includes(yyyymm)) {
    budgetSettings.materialisedMonths.push(yyyymm);
  }
  await saveBudgetLocal();
  _syncQueue?.enqueue();
  return fresh;
}

// ── Aggregations (for the dashboard hero card) ─────────────────────────────
function getMonthInstances(yyyymm) {
  return billInstances[yyyymm] || {};
}

function getLeftToPay(yyyymm) {
  const instances = getMonthInstances(yyyymm);
  let total = 0;
  for (const inst of Object.values(instances)) {
    if (inst.skipped) continue;
    if (inst.paidAt)   continue;
    // Saving instances are paper-only — paying them doesn't move money,
    // so they don't belong in the dashboard's "money still owed this month" tile.
    if (inst.kind === 'saving') continue;
    total += (inst.actualAmount ?? inst.expectedAmount) || 0;
  }
  return Math.round(total * 100) / 100;
}

function getPaidSoFar(yyyymm) {
  const instances = getMonthInstances(yyyymm);
  let total = 0;
  for (const inst of Object.values(instances)) {
    if (inst.skipped) continue;
    if (!inst.paidAt) continue;
    if (inst.kind === 'saving') continue;
    total += (inst.actualAmount ?? inst.expectedAmount) || 0;
  }
  return Math.round(total * 100) / 100;
}

function getNextDueBill(yyyymm, fromIsoDate = null) {
  const instances = getMonthInstances(yyyymm);
  const cutoff    = fromIsoDate || (new Date().toISOString().slice(0, 10));
  let next = null;
  for (const inst of Object.values(instances)) {
    if (inst.skipped || inst.paidAt) continue;
    if (inst.dueDate < cutoff) continue;
    if (!next || inst.dueDate < next.dueDate) next = inst;
  }
  return next; // instance, or null
}

function getUpcomingBills(fromIsoDate = null, withinDays = 14) {
  // Pulls from current month and next month if within the window
  const today = new Date(fromIsoDate || new Date().toISOString().slice(0, 10) + 'T12:00:00');
  const end   = new Date(today.getTime() + withinDays * MS_PER_DAY);
  const months = new Set([_yyyymm(today), _yyyymm(end)]);
  const out = [];
  for (const yyyymm of months) {
    const instances = getMonthInstances(yyyymm);
    for (const inst of Object.values(instances)) {
      if (inst.skipped || inst.paidAt) continue;
      const due = new Date(inst.dueDate + 'T12:00:00');
      if (due >= today && due <= end) {
        out.push({ ...inst, _yyyymm: yyyymm });
      }
    }
  }
  out.sort((a, b) => a.dueDate.localeCompare(b.dueDate));
  return out;
}

// ── Sync merge ─────────────────────────────────────────────────────────────
// Whole-object LWW for templates (matches active mergeItems behaviour)
function shareTargetsMissingBudgetPerm() {
  if (typeof _shareTargets === 'undefined' || !Array.isArray(_shareTargets)) return [];
  return _shareTargets.filter(t => {
    if (!t.households) return false;
    return Object.values(t.households).some(perms => perms && !('budget' in perms));
  });
}

// Offer once per session — UI in turn 2 wires this to a banner.
let _budgetPermBackfillOffered = false;
async function maybeOfferBudgetPermBackfill() {
  if (_budgetPermBackfillOffered) return;
  if (typeof _shareState !== 'undefined' && _shareState) return; // guests don't see this
  const missing = shareTargetsMissingBudgetPerm();
  if (!missing.length) return;
  _budgetPermBackfillOffered = true;
  // UI wiring deferred to turn 2 — for now expose on window for diagnostics
  if (typeof window !== 'undefined') {
    window._budgetPermBackfillTargets = missing;
  }
}

// Apply the backfill once user confirms (called from UI in turn 2).
// Defaults each missing household to 'rw' (matches family preset for budget).
async function applyBudgetPermBackfill() {
  if (typeof _shareTargets === 'undefined' || !Array.isArray(_shareTargets)) return;
  let touched = 0;
  for (const target of _shareTargets) {
    if (!target.households) continue;
    for (const hKey of Object.keys(target.households)) {
      const perms = target.households[hKey];
      if (perms && !('budget' in perms)) {
        perms.budget = 'rw';
        touched++;
      }
    }
  }
  if (touched && typeof saveData === 'function') {
    await saveData();
    if (typeof pushAllSharedData === 'function') {
      try { await pushAllSharedData(); } catch (e) { /* non-fatal */ }
    }
    _syncQueue?.enqueue('Updating share permissions…');
  }
  return touched;
}

// ── Settings setter ────────────────────────────────────────────────────────
async function setBudgetWeekStart(value) {
  if (value !== 'mon' && value !== 'sun') return;
  budgetSettings.weekStart = value;
  await saveBudgetLocal();
  _syncQueue?.enqueue();
}

// ── Diagnostics ────────────────────────────────────────────────────────────
if (typeof window !== 'undefined') {
  window.budgetDiag = function () {
    const yyyymm = _yyyymm(new Date());
    return {
      bills: bills.length,
      activeBills: bills.filter(b => !b.archived).length,
      currentMonth: yyyymm,
      currentMonthInstances: Object.keys(billInstances[yyyymm] || {}).length,
      materialisedMonths: budgetSettings.materialisedMonths.slice(),
      weekStart: budgetSettings.weekStart,
      leftToPay: getLeftToPay(yyyymm),
      paidSoFar: getPaidSoFar(yyyymm),
      nextDue: getNextDueBill(yyyymm),
    };
  };
}


// ═══════════════════════════════════════════════════════════════════════════
//  BUDGET UI — Phase 1
//  Insertion point: in app.js, IMMEDIATELY AFTER the budget-foundations.js
//  block (so this module can call createBill, materialiseMonth, etc.).
//  Around line 12410 if you pasted foundations there.
//
//  Depends on (defined elsewhere): openModal, closeModal, toast,
//  scheduleRender (or render*), _kvEmailHash, _shareState, applyTabPermissions,
//  uid, ic (optional), saveData (for share-perm backfill).
// ═══════════════════════════════════════════════════════════════════════════

// ── Module state (UI-only — not persisted) ─────────────────────────────────
let _budgetViewMonth = null;     // 'YYYY-MM' currently displayed; null = today's month
let _budgetEditingBillId = null; // when bill editor is in edit mode, this is the bill id
let _budgetMarkPaidContext = null; // { yyyymm, billId, expected } during mark-paid modal

// ── View entry point — called by showView('budget', ...) ───────────────────
async function renderBudget() {
  // Default month = today's month
  if (!_budgetViewMonth) {
    const todayMonth = _yyyymm(new Date());
    // Restore an early-rollover override if one is still genuinely ahead of
    // the real calendar; otherwise drop it (the calendar has caught up).
    // Stored in localStorage (device-local) so the sync layer can't wipe it.
    const rolled = _getRolledMonth();
    if (rolled && rolled > todayMonth) {
      _budgetViewMonth = rolled;
    } else {
      if (rolled) _setRolledMonth(null); // calendar caught up — clear override
      _budgetViewMonth = todayMonth;
    }
  }
  // Materialise on first view of any month (idempotent)
  await materialiseMonth(_budgetViewMonth, { persist: true });
  // Auto-roll past-month unpaid saving instances into "paid" so carry-over
  // self-heals across month boundaries even if the user hasn't opened the
  // app for a while. Cheap when there's nothing to roll.
  await _autoRollPastSavingInstances();
  // Self-heal any historical duplicate saving instances (cheap no-op when clean)
  await _dedupeSavingInstances();

  _updateBudgetMonthLabel();
  _refreshBudgetEmptyState();
  _maybeShowBudgetBackfillBanner();
  // Make sure the header action button is correct for the active panel
  // (handles the case where renderBudget is called without panel switching)
  const addBtn = document.getElementById('budget-add-bill-desktop');
  if (addBtn) {
    if (_budgetActivePanel === 'spend') {
      addBtn.innerHTML = '<svg class="icon" aria-hidden="true"><use href="#i-zap"></use></svg> Quick Add';
      addBtn.setAttribute('onclick', 'openQuickAddSpend()');
    } else if (_budgetActivePanel === 'accounts') {
      addBtn.innerHTML = '<svg class="icon" aria-hidden="true"><use href="#i-plus"></use></svg> Add Account';
      addBtn.setAttribute('onclick', 'openAccountEditor()');
    } else {
      addBtn.innerHTML = '<svg class="icon" aria-hidden="true"><use href="#i-plus"></use></svg> Add Bill';
      addBtn.setAttribute('onclick', 'openBillEditor()');
    }
  }

  if (_budgetActivePanel === 'dashboard')      renderBudgetDashboard();
  else if (_budgetActivePanel === 'bills')     renderBudgetBills();
  else if (_budgetActivePanel === 'spend')     renderBudgetSpend();
  else if (_budgetActivePanel === 'accounts')  renderBudgetAccounts();
}

// ── Header — month label + chevrons ────────────────────────────────────────
function _updateBudgetMonthLabel() {
  const el = document.getElementById('budget-month-label');
  if (!el) return;
  const { year, month } = _parseYyyymm(_budgetViewMonth);
  const monthName = new Date(year, month, 1).toLocaleDateString(undefined, { month: 'long', year: 'numeric' });
  el.textContent = monthName;

  // "Today" button visible only when not on current month
  const todayMonth = _yyyymm(new Date());
  const todayBtn = document.getElementById('budget-today-btn');
  if (todayBtn) todayBtn.style.opacity = (_budgetViewMonth === todayMonth) ? '0.4' : '1';
}

async function budgetPrevMonth() {
  const { year, month } = _parseYyyymm(_budgetViewMonth);
  const d = new Date(year, month - 1, 1);
  _budgetViewMonth = _yyyymm(d);
  await renderBudget();
}
async function budgetNextMonth() {
  const { year, month } = _parseYyyymm(_budgetViewMonth);
  const d = new Date(year, month + 1, 1);
  _budgetViewMonth = _yyyymm(d);
  await renderBudget();
}
async function budgetGoToday() {
  _budgetViewMonth = _yyyymm(new Date());
  // Returning to today clears any early-rollover override.
  _setRolledMonth(null);
  await renderBudget();
}

// ── "Start new month" early-rollover button ────────────────────────────────
// Lets the user jump the bill view to next month before the calendar month
// actually ticks over (e.g. when pay lands a day or two early at a weekend).
// Non-destructive: advancing the view materialises a fresh set of unpaid bills
// for next month and auto-rolls split-bill savings, exactly as a natural month
// change would. Spends stay scoped to the real calendar date, so they reset on
// their own when the new month genuinely begins.
//
// Visibility: shown only while viewing today's month, and only from the 25th
// onward OR once every bill in the current month is paid. Dismissed (hidden)
// for the rest of that window once pressed; reappears next month.
function _shouldShowNewMonthButton() {
  if (typeof canWrite === 'function' && !canWrite('budget')) return false;
  const todayMonth = _yyyymm(new Date());
  // Only offer it from the current real month — not while browsing past/future.
  if (_budgetViewMonth !== todayMonth) return false;
  // Already dismissed for this month?
  if (_getNewMonthDismissed() === todayMonth) return false;
  const dayOfMonth = new Date().getDate();
  const allPaid    = getLeftToPay(todayMonth) <= 0
                  && Object.keys(getMonthInstances(todayMonth)).length > 0;
  return dayOfMonth >= 25 || allPaid;
}

function _refreshNewMonthButton() {
  const btn = document.getElementById('budget-start-new-month-btn');
  if (!btn) return;
  btn.style.display = _shouldShowNewMonthButton() ? 'inline-flex' : 'none';
}

async function budgetStartNewMonth() {
  if (typeof canWrite === 'function' && !canWrite('budget')) {
    if (typeof showLockBanner === 'function') showLockBanner('budget');
    return;
  }

  const fromMonth = _yyyymm(new Date());                 // month being closed (real "today")
  const { year, month } = _parseYyyymm(_budgetViewMonth);
  const toMonth   = _yyyymm(new Date(year, month + 1, 1)); // month being entered

  // Make sure both months' instances exist before we try to mark them paid.
  await materialiseMonth(fromMonth, { persist: false });
  await materialiseMonth(toMonth,   { persist: false });

  // Advance the split-bill saving counts: mark the saving instance PAID for
  // both the month being closed and the month being entered, for every active
  // split bill. Idempotent — skips payment months, missing instances, and
  // instances already paid, so pressing twice (or the calendar later catching
  // up) never double-credits.
  for (const tpl of bills) {
    if (tpl.archived) continue;
    if (tpl.paymentStrategy !== 'split') continue;
    if (!tpl.splitInto || !tpl.splitInto.count) continue;
    for (const ym of [fromMonth, toMonth]) {
      const { year: y, month: m } = _parseYyyymm(ym);
      // Only saving months get credited; the payment month is paid via the
      // normal bills list, not here.
      if (!_isSplitBillSavingMonth(tpl, y, m)) continue;
      const inst = _getInstance(ym, tpl.id);
      if (!inst || inst.kind !== 'saving') continue;
      if (inst.paidAt || inst.skipped) continue;
      await markBillPaid(ym, tpl.id, { dueDate: inst.dueDate });
    }
  }

  // Persist the early-rollover so it survives a refresh: the view jumps to the
  // entering month and stays there until the real calendar catches up (or the
  // user taps Today). Stored in localStorage (device-local) so the encrypted
  // budget sync can't overwrite it. Also remember the month we left so the
  // button hides for the rest of this window.
  _setRolledMonth(toMonth);
  _setNewMonthDismissed(fromMonth);
  _budgetViewMonth = toMonth;

  await renderBudget();
  if (typeof toast === 'function') toast('Started next month — bills reset, savings advanced');
}

// ── Empty state ────────────────────────────────────────────────────────────
function _refreshBudgetEmptyState() {
  // Empty only if NEITHER bills NOR spend categories NOR accounts exist.
  // Each panel manages its own per-panel empty state separately.
  const noBills    = !bills.some(b => !b.archived);
  const noCats     = !budgetCategories.some(c => !c.archived);
  const noAccounts = !budgetAccounts.some(a => !a.archived);
  const isEmpty = noBills && noCats && noAccounts;

  const emptyEl    = document.getElementById('budget-empty');
  const dashEl     = document.getElementById('budget-panel-dashboard');
  const billsEl    = document.getElementById('budget-panel-bills');
  const spendEl    = document.getElementById('budget-panel-spend');
  const accountsEl = document.getElementById('budget-panel-accounts');
  const subnavEl   = document.getElementById('budget-subnav');

  if (emptyEl)    emptyEl.style.display    = (isEmpty && _budgetActivePanel === 'dashboard') ? 'block' : 'none';
  // Dashboard hides when isEmpty so the larger budget-empty hero can take its
  // place. Other panels stay live even on empty accounts — they have their
  // own per-panel empty states (e.g. budget-spend-empty, "No accounts yet"),
  // and we want the user to be able to tab between them from the subnav.
  if (dashEl)     dashEl.style.display     = isEmpty ? 'none' : (_budgetActivePanel === 'dashboard' ? 'block' : 'none');
  if (billsEl)    billsEl.style.display    = (_budgetActivePanel === 'bills'     ? 'block' : 'none');
  if (spendEl)    spendEl.style.display    = (_budgetActivePanel === 'spend'     ? 'block' : 'none');
  if (accountsEl) accountsEl.style.display = (_budgetActivePanel === 'accounts'  ? 'block' : 'none');
  // Subnav stays visible at all times so the user can navigate between Bills,
  // Spend, and Accounts even with no data yet. Previously hidden on empty
  // accounts, which left users stranded with only the Basic Mode link.
  if (subnavEl)   subnavEl.style.display   = 'flex';
}

// ── Sub-nav switch ─────────────────────────────────────────────────────────
function budgetSwitchPanel(name) {
  _budgetActivePanel = name;
  document.querySelectorAll('.budget-subnav-btn').forEach(btn => {
    const active = btn.dataset.panel === name;
    btn.classList.toggle('active', active);
    btn.style.background = active ? 'var(--surface)' : 'transparent';
    btn.style.color      = active ? 'var(--text)'    : 'var(--muted)';
  });
  // Basic Mode link active state — toggled separately because it's a text
  // link, not a tab pill.
  document.querySelectorAll('.budget-basic-link').forEach(el => {
    el.classList.toggle('active', el.dataset.panel === name);
  });
  toggle('budget-panel-dashboard', (name === 'dashboard'), 'block');
  toggle('budget-panel-bills', (name === 'bills'), 'block');
  const spendPanel = document.getElementById('budget-panel-spend');
  if (spendPanel) spendPanel.style.display = (name === 'spend') ? 'block' : 'none';
  const accountsPanel = document.getElementById('budget-panel-accounts');
  if (accountsPanel) accountsPanel.style.display = (name === 'accounts') ? 'block' : 'none';
  const basicPanel = document.getElementById('budget-panel-basic');
  if (basicPanel) basicPanel.style.display = (name === 'basic') ? 'block' : 'none';
  // Sync budget-empty visibility with the active panel. It's the dashboard's
  // empty-state hero ("No bills yet" with CTA) and shouldn't bleed onto other
  // panels. Recompute isEmpty here rather than caching from renderBudget —
  // this function gets called from many places with stale state otherwise.
  const _emptyEl = document.getElementById('budget-empty');
  if (_emptyEl) {
    const _isEmpty = !bills.some(b => !b.archived) && !budgetCategories.some(c => !c.archived) && !budgetAccounts.some(a => !a.archived);
    _emptyEl.style.display = (_isEmpty && name === 'dashboard') ? 'block' : 'none';
    // When on the dashboard with empty data, hide the dashboard panel so the
    // empty hero takes its place. _refreshBudgetEmptyState already does this
    // on initial render, but switching panels in JS bypasses it.
    if (_isEmpty && name === 'dashboard') {
      const _dashEl = document.getElementById('budget-panel-dashboard');
      if (_dashEl) _dashEl.style.display = 'none';
    }
  }
  // Header action button — context-sensitive per panel
  const addBtn = document.getElementById('budget-add-bill-desktop');
  if (addBtn) {
    if (name === 'spend') {
      addBtn.innerHTML = '<svg class="icon" aria-hidden="true"><use href="#i-zap"></use></svg> Quick Add';
      addBtn.setAttribute('onclick', 'openQuickAddSpend()');
    } else if (name === 'accounts') {
      addBtn.innerHTML = '<svg class="icon" aria-hidden="true"><use href="#i-plus"></use></svg> Add Account';
      addBtn.setAttribute('onclick', 'openAccountEditor()');
    } else if (name === 'basic') {
      // Read-only view — hide the action button entirely
      addBtn.style.display = 'none';
    } else {
      addBtn.innerHTML = '<svg class="icon" aria-hidden="true"><use href="#i-plus"></use></svg> Add Bill';
      addBtn.setAttribute('onclick', 'openBillEditor()');
    }
    // Re-show the button when leaving basic mode
    if (name !== 'basic') addBtn.style.display = '';
  }
  // Update FAB so mobile users get the panel-specific action
  if (typeof updateFab === 'function' && _currentView === 'budget') updateFab('budget');
  if (name === 'dashboard')      renderBudgetDashboard();
  else if (name === 'bills')     renderBudgetBills();
  else if (name === 'spend')     renderBudgetSpend();
  else if (name === 'accounts')  renderBudgetAccounts();
  else if (name === 'basic')     renderBudgetBasicMode();
}

// ── Currency formatting ────────────────────────────────────────────────────
function _daysFromToday(iso) {
  const today = new Date(); today.setHours(0, 0, 0, 0);
  const d = new Date(iso + 'T12:00:00'); d.setHours(0, 0, 0, 0);
  return Math.round((d.getTime() - today.getTime()) / MS_PER_DAY);
}

function _relativeDay(iso) {
  const n = _daysFromToday(iso);
  if (n === 0)  return 'Today';
  if (n === 1)  return 'Tomorrow';
  if (n === -1) return 'Yesterday';
  if (n > 0)    return `In ${n} days`;
  return `${Math.abs(n)} days ago`;
}

// ── Dashboard render ───────────────────────────────────────────────────────
function renderBudgetDashboard() {
  const yyyymm = _budgetViewMonth;
  const left   = getLeftToPay(yyyymm);
  const paid   = getPaidSoFar(yyyymm);
  const next   = getNextDueBill(yyyymm);

  const heroAmount = document.getElementById('budget-hero-amount');
  const heroPaid   = document.getElementById('budget-hero-paid');
  const heroNext   = document.getElementById('budget-hero-next');
  if (heroAmount) heroAmount.textContent = _money(left);
  if (heroPaid)   heroPaid.textContent   = _money(paid);
  if (heroNext) {
    if (next) {
      const tpl = bills.find(b => b.id === next.billId);
      heroNext.textContent = `${tpl?.name || 'Bill'} · ${_shortDate(next.dueDate)}`;
    } else {
      heroNext.textContent = 'All paid';
    }
  }

  // Upcoming bills list (next 14 days, current view month or anchor today if viewing past/future month)
  const upcomingHost = document.getElementById('budget-upcoming-list');
  if (upcomingHost) {
    const todayMonth = _yyyymm(new Date());
    const fromIso = (yyyymm === todayMonth) ? null : `${yyyymm}-01`;
    const items = getUpcomingBills(fromIso, 14);
    if (items.length === 0) {
      upcomingHost.innerHTML = '<div style="padding:24px 16px;text-align:center;color:var(--muted);font-size:13px">No bills due in the next 14 days</div>';
    } else {
      upcomingHost.innerHTML = items.map(inst => _renderBillRow(inst, { showRelative: true })).join('');
    }
  }

  // Category tiles (Phase 2) — replace the placeholder cards
  const tilesHost = document.getElementById('budget-dashboard-tiles');
  const tilesEmpty = document.getElementById('budget-dashboard-tiles-empty');
  if (tilesHost && tilesEmpty) {
    const cats = getActiveBudgetCategories();
    if (cats.length === 0) {
      tilesHost.style.display = 'none';
      tilesEmpty.style.display = 'block';
    } else {
      tilesEmpty.style.display = 'none';
      tilesHost.style.display = 'flex';
      // Dashboard tiles use the bill view's month, not the spend ref date
      const { year, month } = _parseYyyymm(yyyymm);
      const startIso = `${yyyymm}-01`;
      const endDate  = new Date(year, month + 1, 0);
      const endIso   = `${yyyymm}-${String(endDate.getDate()).padStart(2, '0')}`;
      tilesHost.innerHTML = cats.map(cat => _renderCategoryTile(cat, startIso, endIso, 'month', false)).join('');
    }
  }

  // Cash flow chart (Phase 3) — replaces the Phase 3 placeholder
  if (typeof renderCashFlowChart === 'function') renderCashFlowChart();
  // Augment the hero card with projected balance + low-point
  if (typeof _augmentHeroWithProjection === 'function') _augmentHeroWithProjection();

  // Early-rollover button in the hero box
  _refreshNewMonthButton();
}

// ── Bills panel render ─────────────────────────────────────────────────────
function renderBudgetBills() {
  const yyyymm    = _budgetViewMonth;
  const instances = getMonthInstances(yyyymm);
  const allRaw    = Object.values(instances);

  // Phase 5c: split-strategy bills get their own section at the bottom.
  // Their SAVING instances are pulled out of the regular Due/Paid/Skipped
  // lists, but their PAYMENT-month instance still appears in the regular
  // sections too (so the user sees the upcoming bill alongside their
  // lump-sum bills). Once paid, the payment instance disappears from Due
  // and stays in the multi-month timeline; the carry-over for that bill
  // also resets.
  const all     = allRaw.filter(i => i.kind !== 'saving');
  const due     = all.filter(i => !i.skipped && !i.paidAt);
  const paid    = all.filter(i => !i.skipped &&  i.paidAt);
  const skipped = all.filter(i =>  i.skipped);

  // Sort: due by dueDate asc, paid by paidAt desc, skipped by dueDate asc
  due.sort((a, b)     => a.dueDate.localeCompare(b.dueDate));
  paid.sort((a, b)    => (b.paidAt || '').localeCompare(a.paidAt || ''));
  skipped.sort((a, b) => a.dueDate.localeCompare(b.dueDate));

  // Multi-month bills: any non-archived split-strategy template with at
  // least one instance somewhere in its current cycle, OR with a payment
  // expected in the current view month, OR with the view month falling
  // inside its cycle. We surface them in the new section regardless of
  // whether THIS month happens to be a saving/payment month for the bill.
  const { year, month } = _parseYyyymm(yyyymm);
  const multiMonthTemplates = bills.filter(tpl => {
    if (tpl.archived) return false;
    if (tpl.paymentStrategy !== 'split') return false;
    if (!tpl.splitInto || !tpl.splitInto.count) return false;
    return true;
  });

  // "Other bills": non-archived templates not active this month that AREN'T
  // split-strategy (those are in the multi-month section) and that don't
  // have any instance in the current month.
  const billIdsWithAnyInstanceThisMonth = new Set(allRaw.map(i => i.billId));
  const splitBillIds = new Set(multiMonthTemplates.map(b => b.id));
  const inactiveTemplates = bills.filter(b =>
       !b.archived
    && !splitBillIds.has(b.id)
    && !shouldBeDueInMonth(b, year, month)
    && !billIdsWithAnyInstanceThisMonth.has(b.id)
  );
  const archivedTemplates = bills.filter(b => b.archived);

  // Populate sections (lump-sum bills only)
  _renderBillSection('budget-bills-due',     due.map(i => _renderBillRow(i)),     due.length);
  _renderBillSection('budget-bills-paid',    paid.map(i => _renderBillRow(i)),    paid.length);
  _renderBillSection('budget-bills-skipped', skipped.map(i => _renderBillRow(i)), skipped.length);

  // Hide the legacy "Saving up" section if it's still in the DOM
  const savingSection = document.getElementById('budget-bills-savingup-section');
  if (savingSection) savingSection.style.display = 'none';

  // Multi-month bills section
  const mmSection = document.getElementById('budget-bills-multimonth-section');
  const mmList    = document.getElementById('budget-bills-multimonth-list');
  const mmCount   = document.getElementById('budget-bills-multimonth-count');
  if (mmSection && mmList && mmCount) {
    if (multiMonthTemplates.length) {
      mmSection.style.display = '';
      mmCount.textContent     = multiMonthTemplates.length;
      mmList.innerHTML        = `<div class="bill-list">${multiMonthTemplates.map(_renderMultiMonthBillRow).join('')}</div>`;
    } else {
      mmSection.style.display = 'none';
    }
  }

  // "Other bills" — templates inactive this month
  const otherSection = document.getElementById('budget-bills-other-section');
  const otherList    = document.getElementById('budget-bills-other-list');
  const otherCount   = document.getElementById('budget-bills-other-count');
  if (otherSection && otherList && otherCount) {
    if (inactiveTemplates.length) {
      otherSection.style.display = '';
      otherCount.textContent     = inactiveTemplates.length;
      otherList.innerHTML        = `<div class="bill-list">${inactiveTemplates.map(_renderBillTemplateRow).join('')}</div>`;
    } else {
      otherSection.style.display = 'none';
    }
  }

  // Archived
  const archSection = document.getElementById('budget-bills-archived-section');
  const archList    = document.getElementById('budget-bills-archived-list');
  const archCount   = document.getElementById('budget-bills-archived-count');
  if (archSection && archList && archCount) {
    if (archivedTemplates.length) {
      archSection.style.display = '';
      archCount.textContent     = archivedTemplates.length;
      archList.innerHTML        = `<div class="bill-list">${archivedTemplates.map(_renderBillTemplateRow).join('')}</div>`;
    } else {
      archSection.style.display = 'none';
    }
  }

  // Summary — counts BOTH lump-sum instances and multi-month bills (one
  // count per template, since the timeline encapsulates the cycle).
  // Payment-month split bills appear in both Due and Multi-month sections
  // for UX, but should only be counted ONCE in the total. Subtract those
  // overlaps.
  const summary = document.getElementById('budget-bills-summary');
  if (summary) {
    const lumpTotal = all.filter(i => !i.skipped)
      .reduce((s, i) => s + ((i.actualAmount ?? i.expectedAmount) || 0), 0);
    // How many of the multi-month templates also have an instance in `all`
    // this month? Those are the duplicates to subtract.
    const splitBillIdsInAll = new Set(
      all.filter(i => splitBillIds.has(i.billId)).map(i => i.billId)
    );
    const totalCount = all.length + multiMonthTemplates.length - splitBillIdsInAll.size;
    let carryNote = '';
    if (typeof getTotalCarryOver === 'function') {
      const co = getTotalCarryOver();
      if (co && co.total > 0 && co.breakdown.length > 0) {
        carryNote = ` <span style="color:var(--accent2)">(including ${_money(co.total)} carrying forward for ${co.breakdown.length} bill${co.breakdown.length === 1 ? '' : 's'})</span>`;
      }
    }
    summary.innerHTML = `${totalCount} bill${totalCount !== 1 ? 's' : ''} this month · expected total <strong style="color:var(--text)">${_money(lumpTotal)}</strong>${carryNote}`;
  }
}

function _renderBillSection(idPrefix, rowsHtml, count) {
  const section = document.getElementById(`${idPrefix}-section`);
  const list    = document.getElementById(`${idPrefix}-list`);
  const counter = document.getElementById(`${idPrefix}-count`);
  if (!section || !list) return;
  if (count === 0) { section.style.display = 'none'; return; }
  section.style.display = '';
  if (counter) counter.textContent = count;
  list.innerHTML = `<div class="bill-list">${rowsHtml.join('')}</div>`;
}

// ── Bill instance row (for due/paid/skipped/upcoming) ──────────────────────
function _renderBillRow(inst, opts = {}) {
  const tpl = bills.find(b => b.id === inst.billId);
  const name = tpl?.name || 'Unknown bill';
  const variableTag = tpl?.variableAmount ? '<span class="bill-tag">~</span>' : '';
  const amount = inst.actualAmount ?? inst.expectedAmount;
  const isPaid    = !!inst.paidAt;
  const isSkipped = !!inst.skipped;
  const today     = new Date().toISOString().slice(0, 10);
  // Phase 5b: saving instances are paper-only set-asides for split bills.
  // Visually distinct (piggy-bank icon, accent2 colour, "Saving" tag).
  const isSaving = inst.kind === 'saving';
  // Saving instances aren't "overdue" — the user has the whole month to
  // mark them set aside, and unpaid ones auto-roll at month boundary.
  const isOverdue = !isPaid && !isSkipped && !isSaving && inst.dueDate < today;

  let stateClass = '';
  if (isPaid)         stateClass = 'is-paid';
  else if (isSkipped) stateClass = 'is-skipped';
  else if (isOverdue) stateClass = 'is-overdue';
  else                stateClass = 'is-due';

  const day = inst.dueDate.slice(8, 10).replace(/^0/, '');
  const yyyymm = inst._yyyymm || _yyyymmFromString(inst.dueDate);

  let metaText = '';
  if (opts.showRelative) {
    metaText = _relativeDay(inst.dueDate);
  } else if (isPaid) {
    const paidWhen = new Date(inst.paidAt);
    if (isSaving && (inst.source === 'split-saving-autoroll' || inst.source === 'split-saving-backfill')) {
      metaText = `Set aside (auto)`;
    } else {
      metaText = `Paid ${paidWhen.toLocaleDateString(undefined, { day: 'numeric', month: 'short' })}`;
    }
  } else if (isSkipped) {
    metaText = 'Skipped';
  } else if (isSaving) {
    // Saving instances have all month to be paid; no overdue framing.
    metaText = 'Set aside this month';
  } else if (isOverdue) {
    metaText = `${Math.abs(_daysFromToday(inst.dueDate))} days overdue`;
  } else {
    metaText = _relativeDay(inst.dueDate);
  }

  // Saving-instance specific meta: show the next payment month so it's
  // clear what we're saving for. Replace the standard splitMeta entirely.
  let splitMeta = '';
  if (isSaving && tpl) {
    const co = getBillCarryOver(tpl);
    if (co && co.nextDueIso) {
      const dueWhen = new Date(co.nextDueIso + 'T12:00:00').toLocaleDateString(undefined, { month: 'short', year: 'numeric' });
      splitMeta = ` · <span style="color:var(--accent2)">saving for ${dueWhen}</span>`;
    }
  } else if (tpl && tpl.paymentStrategy === 'split' && !isPaid && !isSkipped) {
    // Payment-instance of a split bill — show how much is already set aside
    const co = getBillCarryOver(tpl);
    if (co && co.accrued > 0) {
      splitMeta = ` · <span style="color:var(--accent2)">${_money(co.accrued)} already saved</span>`;
    }
  }

  // Action buttons depend on state
  let actions = '';
  if (isPaid) {
    actions = `<button class="bill-action-btn" onclick="event.stopPropagation();handleBillUnpay('${yyyymm}','${inst.billId}','${inst.dueDate}')" title="Unmark paid"><svg aria-hidden="true"><use href="#i-refresh-cw"></use></svg></button>`;
  } else if (isSkipped) {
    actions = `<button class="bill-action-btn" onclick="event.stopPropagation();handleBillUnskip('${yyyymm}','${inst.billId}','${inst.dueDate}')" title="Unskip"><svg aria-hidden="true"><use href="#i-refresh-cw"></use></svg></button>`;
  } else {
    actions =
      `<button class="bill-action-btn bill-action-paid" onclick="event.stopPropagation();handleBillPay('${yyyymm}','${inst.billId}','${inst.dueDate}')" title="${isSaving ? 'Mark set aside' : 'Mark paid'}"><svg aria-hidden="true"><use href="#i-check"></use></svg></button>` +
      `<button class="bill-action-btn bill-action-skip" onclick="event.stopPropagation();handleBillSkip('${yyyymm}','${inst.billId}','${inst.dueDate}')" title="Skip this month"><svg aria-hidden="true"><use href="#i-x"></use></svg></button>` +
      `<button class="bill-action-btn bill-action-edit" onclick="event.stopPropagation();openBillEditor('${inst.billId}')" title="Edit bill"><svg aria-hidden="true"><use href="#i-pencil"></use></svg></button>`;
  }

  // Saving instances: piggy-bank icon in place of the day, accent2-coloured
  // amount, and a "SAVING" tag next to the name.
  const dayCell = isSaving
    ? `<div class="bill-day" style="color:var(--accent2);border-color:rgba(91,141,238,0.4);background:rgba(91,141,238,0.08)"><svg style="width:14px;height:14px" aria-hidden="true"><use href="#i-piggy-bank"></use></svg></div>`
    : `<div class="bill-day">${day}</div>`;
  const savingTag = isSaving
    ? `<span class="bill-tag" style="background:rgba(91,141,238,0.15);color:var(--accent2);border-color:rgba(91,141,238,0.3);font-size:9px">SAVING</span>`
    : '';
  const amountStyle = isSaving ? 'color:var(--accent2)' : '';

  return `
    <div class="bill-row ${stateClass}" onclick="openBillEditor('${inst.billId}')">
      ${dayCell}
      <div class="bill-info">
        <div class="bill-name">${_escapeHtml(name)} ${variableTag}${savingTag}</div>
        <div class="bill-meta">${metaText}${splitMeta}</div>
      </div>
      <div class="bill-amount ${tpl?.variableAmount && !isPaid && !isSaving ? 'is-variable' : ''}" style="${amountStyle}">${_money(amount)}</div>
      <div class="bill-actions">${actions}</div>
    </div>`;
}

// ── Bill template row (for "Other bills" + "Archived" sections) ────────────
function _renderBillTemplateRow(tpl) {
  const freq = _frequencyLabel(tpl);
  let splitMeta = '';
  if (tpl.paymentStrategy === 'split' && !tpl.archived) {
    const co = getBillCarryOver(tpl);
    if (co) {
      splitMeta = ` · <span style="color:var(--accent2)">${co.slot}/${co.totalSlots} · ${_money(co.accrued)} saved</span>`;
    }
  }
  return `
    <div class="bill-row ${tpl.archived ? 'is-skipped' : ''}" onclick="openBillEditor('${tpl.id}')">
      <div class="bill-day">${tpl.dayOfMonth}</div>
      <div class="bill-info">
        <div class="bill-name">${_escapeHtml(tpl.name)}</div>
        <div class="bill-meta">${freq}${tpl.archived ? ' · archived' : ''}${splitMeta}</div>
      </div>
      <div class="bill-amount">${_money(tpl.amount)}</div>
      <div class="bill-actions">
        <button class="bill-action-btn bill-action-edit" onclick="event.stopPropagation();openBillEditor('${tpl.id}')" title="Edit"><svg aria-hidden="true"><use href="#i-pencil"></use></svg></button>
      </div>
    </div>`;
}

// Multi-month bill row — for split-strategy bills shown in their own
// section at the bottom of the Bills panel. Shows the bill name, total
// amount, and a status line summarising the saving cycle (months saved /
// total, payment date, months remaining). Tapping opens the timeline
// modal where the user can interact with each month's instance.
function _renderMultiMonthBillRow(tpl) {
  const co = getBillCarryOver(tpl);
  if (!co) return '';
  const dueLabel = co.nextDueIso
    ? new Date(co.nextDueIso + 'T12:00:00').toLocaleDateString(undefined, { day: 'numeric', month: 'short' })
    : null;

  // Saving months remaining = how many set-asides are still to be made before
  // the bill pays. Driven by the saved count (single source of truth), NOT
  // calendar distance — so pressing "Start new month" advances the saved count
  // and this figure ticks down by one in step.
  let monthsLeftLabel = '';
  const savingMonthsLeft = Math.max(0, co.totalSlots - co.slot);
  if (co.currentMonthIsPayment) {
    monthsLeftLabel = 'pays this month';
  } else if (co.nextDueIso) {
    const today = new Date();
    const next  = new Date(co.nextDueIso + 'T12:00:00');
    const monthsDiff = (next.getFullYear() - today.getFullYear()) * 12
                     + (next.getMonth()    - today.getMonth());
    if (monthsDiff <= 0) {
      monthsLeftLabel = 'overdue';
    } else if (savingMonthsLeft === 0) {
      monthsLeftLabel = 'fully saved';
    } else if (savingMonthsLeft === 1) {
      monthsLeftLabel = '1 month left';
    } else {
      monthsLeftLabel = `${savingMonthsLeft} months left`;
    }
  }

  // Status line: months saved · pays Date · monthsLeft
  const parts = [`${co.slot}/${co.totalSlots} mo saved`];
  if (co.currentMonthIsPayment) {
    if (dueLabel) parts.push(`pays ${dueLabel} (this month)`);
  } else {
    if (dueLabel)        parts.push(`pays ${dueLabel}`);
    if (monthsLeftLabel) parts.push(monthsLeftLabel);
  }
  const status = parts.join(' · ');

  // Per-period figure shown subtly so the user knows the monthly amount
  const subAmt = `<span style="font-size:11px;color:var(--muted);font-family:var(--mono);display:block;margin-top:2px">${_money(co.perPeriod)}/mo</span>`;

  return `
    <div class="bill-row" style="border-left:2px solid var(--accent2);cursor:pointer" onclick="openMultiMonthTimeline('${tpl.id}')">
      <div class="bill-day" style="color:var(--accent2);border-color:rgba(91,141,238,0.4);background:rgba(91,141,238,0.08)">
        <svg style="width:14px;height:14px" aria-hidden="true"><use href="#i-piggy-bank"></use></svg>
      </div>
      <div class="bill-info">
        <div class="bill-name">${_escapeHtml(tpl.name)}</div>
        <div class="bill-meta">${status}</div>
      </div>
      <div style="text-align:right">
        <div class="bill-amount">${_money(tpl.amount)}</div>
        ${subAmt}
      </div>
      <div class="bill-actions">
        <button class="bill-action-btn bill-action-edit" onclick="event.stopPropagation();openBillEditor('${tpl.id}')" title="Edit bill"><svg aria-hidden="true"><use href="#i-pencil"></use></svg></button>
      </div>
    </div>`;
}

// ── Saving-up bill row (split bills not due this month, accumulating) ──
// Shows the per-period set-aside amount as the prominent figure (this is
// what you should think of as "due" for budgeting purposes), with the
// total target, slot in cycle, and next-due date as meta.
function _renderSavingUpBillRow(tpl, viewYyyymm) {
  const progress = getBillCycleProgressForMonth(tpl, viewYyyymm);
  if (!progress) return '';
  const dueLabel = progress.nextDueIso
    ? new Date(progress.nextDueIso + 'T12:00:00').toLocaleDateString(undefined, { day: 'numeric', month: 'short', year: 'numeric' })
    : 'no future due date';
  const unitLabel = progress.unit === 'week' ? 'wk' : 'mo';
  const meta = `${progress.slot}/${progress.totalSlots} ${unitLabel} · ${_money(progress.accrued)} of ${_money(progress.target)} saved · due ${dueLabel}`;
  return `
    <div class="bill-row" style="border-left:2px solid var(--accent2)" onclick="openBillEditor('${tpl.id}')">
      <div class="bill-day" style="color:var(--accent2);border-color:rgba(91,141,238,0.4);background:rgba(91,141,238,0.08)">
        <svg style="width:14px;height:14px" aria-hidden="true"><use href="#i-piggy-bank"></use></svg>
      </div>
      <div class="bill-info">
        <div class="bill-name">${_escapeHtml(tpl.name)}</div>
        <div class="bill-meta">${meta}</div>
      </div>
      <div class="bill-amount" style="color:var(--accent2)">${_money(progress.perPeriod)}</div>
      <div class="bill-actions">
        <button class="bill-action-btn bill-action-edit" onclick="event.stopPropagation();openBillEditor('${tpl.id}')" title="Edit"><svg aria-hidden="true"><use href="#i-pencil"></use></svg></button>
      </div>
    </div>`;
}

function _frequencyLabel(tpl) {
  const f = tpl.frequency || { unit: 'month', interval: 1 };
  if (f.unit === 'year') return 'Annual';
  const interval = f.interval || 1;
  if (interval === 1)  return 'Monthly';
  if (interval === 3)  return 'Every 3 months';
  if (interval === 6)  return 'Every 6 months';
  if (interval === 12) return 'Annual';
  return `Every ${interval} months`;
}

async function handleBillPay(yyyymm, billId, dueDate = null) {
  const tpl = bills.find(b => b.id === billId);
  const inst = _getInstance(yyyymm, billId, dueDate);
  if (!tpl || !inst) return;
  // Saving instances are paper-only set-asides — fixed amount, no prompt,
  // distinct toast wording so the user understands it's a budget action,
  // not a payment that left the account.
  if (inst.kind === 'saving') {
    await markBillPaid(yyyymm, billId, { dueDate: inst.dueDate });
    await renderBudget();
    toast(`${_money(inst.expectedAmount)} set aside for ${tpl.name}`);
    return;
  }
  // For variable bills, prompt for actual amount
  if (tpl.variableAmount) {
    _budgetMarkPaidContext = { yyyymm, billId, dueDate: inst.dueDate, expected: inst.expectedAmount };
    document.getElementById('bmp-subtitle').textContent = `${tpl.name} — due ${_shortDate(inst.dueDate)}`;
    const amtIn = document.getElementById('bmp-amount');
    amtIn.value = inst.actualAmount != null ? inst.actualAmount : inst.expectedAmount;
    document.getElementById('bmp-expected-hint').textContent = `Estimated: ${_money(inst.expectedAmount)}`;
    openModal('bill-mark-paid-modal');
    setTimeout(() => amtIn.select(), 50);
    return;
  }
  // Fixed-amount bill — mark paid directly
  await markBillPaid(yyyymm, billId, { dueDate: inst.dueDate });
  await renderBudget();
  toast(`Marked ${tpl.name} paid`);
}

async function confirmMarkBillPaid() {
  const ctx = _budgetMarkPaidContext;
  if (!ctx) return;
  const amt = parseFloat(document.getElementById('bmp-amount').value);
  if (isNaN(amt) || amt < 0) { toast('Enter a valid amount'); return; }
  await markBillPaid(ctx.yyyymm, ctx.billId, { actualAmount: amt, dueDate: ctx.dueDate });
  closeModal('bill-mark-paid-modal');
  _budgetMarkPaidContext = null;
  await renderBudget();
  toast('Bill marked paid');
}

async function handleBillUnpay(yyyymm, billId, dueDate = null) {
  await markBillUnpaid(yyyymm, billId, dueDate);
  await renderBudget();
}

async function handleBillSkip(yyyymm, billId, dueDate = null) {
  await skipBillInstance(yyyymm, billId, dueDate);
  await renderBudget();
}

async function handleBillUnskip(yyyymm, billId, dueDate = null) {
  await unskipBillInstance(yyyymm, billId, dueDate);
  await renderBudget();
}

// ── Bill editor modal — open / save / archive ──────────────────────────────
function openBillEditor(billId = null) {
  _budgetEditingBillId = billId;
  const tpl = billId ? bills.find(b => b.id === billId) : null;
  document.getElementById('bill-editor-mode-label').textContent = tpl ? 'Edit Bill' : 'Add Bill';
  document.getElementById('bill-save-label').textContent        = tpl ? 'Save Changes' : 'Save Bill';
  toggle('bill-archive-btn', tpl, 'inline-flex');
  // Phase 5: also show the Delete button alongside Archive when editing
  const delBtn = document.getElementById('bill-delete-btn');
  if (delBtn) delBtn.style.display = tpl ? 'inline-flex' : 'none';

  document.getElementById('bill-name').value          = tpl?.name      || '';
  document.getElementById('bill-amount').value        = tpl?.amount    ?? '';
  document.getElementById('bill-variable').checked    = !!tpl?.variableAmount;
  document.getElementById('bill-day-of-month').value  = tpl?.dayOfMonth ?? 1;
  document.getElementById('bill-notes').value         = tpl?.notes     || '';

  // Frequency presets
  const freq = tpl?.frequency || { unit: 'month', interval: 1, anchorMonth: null };
  let preset = 'monthly';
  if (freq.unit === 'year') preset = 'annual';
  else if (freq.interval === 3)  preset = 'quarterly';
  else if (freq.interval === 6)  preset = 'six_monthly';
  else if (freq.interval === 1)  preset = 'monthly';
  else                           preset = 'custom';
  document.getElementById('bill-frequency-preset').value  = preset;
  document.getElementById('bill-anchor-month').value      = String(freq.anchorMonth ?? 0);
  document.getElementById('bill-custom-interval').value   = (preset === 'custom' ? freq.interval : 2);

  // Phase 5: payment strategy + split-into
  const strategy = tpl?.paymentStrategy === 'split' ? 'split' : 'lump';
  const lumpRadio  = document.querySelector('input[name="bill-payment-strategy"][value="lump"]');
  const splitRadio = document.querySelector('input[name="bill-payment-strategy"][value="split"]');
  if (lumpRadio)  lumpRadio.checked  = (strategy === 'lump');
  if (splitRadio) splitRadio.checked = (strategy === 'split');
  // Hidden inputs are now auto-populated by _refreshBillSplitVisibility from
  // the bill's frequency. We don't need to seed them from the template since
  // splitInto is always derived to match the cycle.

  billOnFreqPresetChange();
  _refreshBillVariableHint();
  _refreshBillSplitVisibility();
  openModal('bill-editor-modal');
  setTimeout(() => document.getElementById('bill-name').focus(), 50);
}

function billOnFreqPresetChange() {
  const preset = document.getElementById('bill-frequency-preset').value;
  const anchorRow = document.getElementById('bill-anchor-row');
  const customRow = document.getElementById('bill-custom-interval-row');
  const anchorLabel = document.getElementById('bill-anchor-label');
  const anchorHint  = document.getElementById('bill-anchor-hint');

  const showAnchor = preset !== 'monthly';
  anchorRow.style.display = showAnchor ? 'block' : 'none';
  customRow.style.display = preset === 'custom' ? 'block' : 'none';

  if (showAnchor) {
    if (preset === 'annual') {
      anchorLabel.textContent = 'Month it pays';
      anchorHint.textContent  = 'The month the bill leaves your account each year.';
    } else {
      anchorLabel.textContent = 'First payment month';
      const intervalText = preset === 'quarterly'   ? 'every 3 months'
                        : preset === 'six_monthly'  ? 'every 6 months'
                        : 'every N months';
      anchorHint.textContent  = `The first month the bill leaves your account. Then ${intervalText} after that.`;
    }
  }

  _refreshBillSplitVisibility();
}

// Show or hide the payment-strategy section depending on whether the current
// frequency makes a split strategy meaningful (cycle > 1 month).
function _refreshBillSplitVisibility() {
  const preset    = document.getElementById('bill-frequency-preset')?.value;
  const customInt = parseInt(document.getElementById('bill-custom-interval')?.value, 10);
  const customUnit = document.getElementById('bill-custom-unit')?.value || 'month';
  let freq;
  if (preset === 'monthly')           freq = { unit: 'month', interval: 1 };
  else if (preset === 'quarterly')    freq = { unit: 'month', interval: 3 };
  else if (preset === 'six_monthly')  freq = { unit: 'month', interval: 6 };
  else if (preset === 'annual')       freq = { unit: 'year',  interval: 1 };
  else if (preset === 'custom') {
    if (customUnit === 'year')        freq = { unit: 'year',  interval: Math.max(1, customInt || 1) };
    else                              freq = { unit: 'month', interval: Math.max(1, customInt || 2) };
  } else                              freq = { unit: 'month', interval: Math.max(1, customInt || 2) };

  const section   = document.getElementById('bill-payment-strategy-section');
  const splitRow  = document.getElementById('bill-split-row');
  const canSplit  = _billCanSplit(freq);
  if (section)  section.style.display  = canSplit ? 'block' : 'none';

  // If split isn't applicable (monthly bills), force lump and bail.
  if (!canSplit) {
    const lumpRadio = document.querySelector('input[name="bill-payment-strategy"][value="lump"]');
    if (lumpRadio) lumpRadio.checked = true;
    if (splitRow) splitRow.style.display = 'none';
    return;
  }
  // Show the split row only when "split" is selected.
  const splitRadio = document.querySelector('input[name="bill-payment-strategy"][value="split"]');
  if (splitRow) splitRow.style.display = splitRadio?.checked ? 'block' : 'none';

  // Auto-derive split from frequency. The user can no longer override —
  // keeping the split locked to the cycle keeps the math consistent across
  // all cycles, and means a quarterly bill always splits across exactly 3
  // months, an annual across 12, and so on.
  const auto = _suggestSplitInto(freq) || { unit: 'month', count: 1 };
  const countEl = document.getElementById('bill-split-count');
  const unitEl  = document.getElementById('bill-split-unit');
  if (countEl) countEl.value = String(auto.count);
  if (unitEl)  unitEl.value  = auto.unit;

  // Populate the read-only info text. Computes the per-period amount from
  // whatever's currently in the amount field.
  const infoEl = document.getElementById('bill-split-info');
  if (infoEl) {
    const amt = parseFloat(document.getElementById('bill-amount')?.value);
    const unitLabel = auto.unit === 'week'
      ? (auto.count === 1 ? 'week' : 'weeks')
      : (auto.count === 1 ? 'month' : 'months');
    let perLine = '';
    if (!isNaN(amt) && amt > 0) {
      const per = Math.round((amt / auto.count) * 100) / 100;
      perLine = `<div style="margin-top:4px;color:var(--text)"><strong>${_money(per)}</strong> per ${auto.unit} for ${auto.count} ${unitLabel}, then the full ${_money(amt)} comes out on the payment month.</div>`;
    }
    infoEl.innerHTML = `Splits automatically across <strong style="color:var(--text)">${auto.count} ${unitLabel}</strong> to match the bill's cycle.${perLine}`;
  }
}

function _refreshBillVariableHint() {
  const checked = document.getElementById('bill-variable').checked;
  toggle('bill-amount-hint', checked, 'block');
}
document.addEventListener('change', e => {
  if (e.target?.id === 'bill-variable') _refreshBillVariableHint();
  if (e.target?.name === 'bill-payment-strategy') _refreshBillSplitVisibility();
  if (e.target?.id === 'bill-custom-interval') _refreshBillSplitVisibility();
  if (e.target?.id === 'bill-custom-unit')     _refreshBillSplitVisibility();
});
// Live-update the per-period figure as the user types in the amount field.
document.addEventListener('input', e => {
  if (e.target?.id === 'bill-amount') _refreshBillSplitVisibility();
});

async function saveBillFromEditor() {
  const name = (document.getElementById('bill-name').value || '').trim();
  if (!name) { toast('Bill needs a name'); return; }
  const amount = parseFloat(document.getElementById('bill-amount').value);
  if (isNaN(amount) || amount < 0) { toast('Enter a valid amount'); return; }
  const dayOfMonth = parseInt(document.getElementById('bill-day-of-month').value, 10);
  if (isNaN(dayOfMonth) || dayOfMonth < 1 || dayOfMonth > 31) { toast('Day must be 1-31'); return; }
  const variableAmount = document.getElementById('bill-variable').checked;
  const notes = (document.getElementById('bill-notes').value || '').trim();

  const preset       = document.getElementById('bill-frequency-preset').value;
  const anchorMonth  = parseInt(document.getElementById('bill-anchor-month').value, 10);
  const customInt    = parseInt(document.getElementById('bill-custom-interval').value, 10);
  let frequency;
  if (preset === 'monthly')           frequency = { unit: 'month', interval: 1, anchorMonth: null };
  else if (preset === 'quarterly')    frequency = { unit: 'month', interval: 3, anchorMonth };
  else if (preset === 'six_monthly')  frequency = { unit: 'month', interval: 6, anchorMonth };
  else if (preset === 'annual')       frequency = { unit: 'year',  interval: 1, anchorMonth };
  else                                frequency = { unit: 'month', interval: Math.max(1, customInt || 2), anchorMonth };

  // Phase 5: payment strategy + split-into. Only relevant when the bill's
  // Phase 5: payment strategy. splitInto is auto-derived from the frequency.
  let paymentStrategy = 'lump';
  let splitInto       = null;
  if (_billCanSplit(frequency)) {
    const stratEl = document.querySelector('input[name="bill-payment-strategy"]:checked');
    paymentStrategy = stratEl?.value === 'split' ? 'split' : 'lump';
    if (paymentStrategy === 'split') {
      splitInto = _suggestSplitInto(frequency);
      if (!splitInto || splitInto.count < 1) {
        toast('Cannot split this bill — frequency too short');
        return;
      }
    }
  }

  const patch = { name, amount, variableAmount, dayOfMonth, notes, frequency, paymentStrategy, splitInto };

  if (_budgetEditingBillId) {
    await updateBill(_budgetEditingBillId, patch);
    toast('Bill updated');
  } else {
    await createBill(patch);
    toast('Bill added');
  }

  // Re-materialise current month so changes show up. This pulls in any new bills,
  // but does not overwrite already-paid instances since materialiseMonth respects existing data.
  await materialiseMonth(_budgetViewMonth, { persist: true });

  closeModal('bill-editor-modal');
  _budgetEditingBillId = null;
  await renderBudget();
}

async function confirmArchiveBill() {
  if (!_budgetEditingBillId) return;
  const tpl = bills.find(b => b.id === _budgetEditingBillId);
  if (!tpl) return;
  if (!confirm(`Archive "${tpl.name}"? Past instances stay; no new ones will be generated.`)) return;
  await archiveBill(_budgetEditingBillId);
  closeModal('bill-editor-modal');
  _budgetEditingBillId = null;
  toast('Bill archived');
  await renderBudget();
}

// Hard delete — wipes the template AND all materialised instances across
// every month. Irreversible. Strong two-step confirmation since this also
// removes any payment history for the bill.
async function confirmDeleteBill() {
  if (!_budgetEditingBillId) return;
  const tpl = bills.find(b => b.id === _budgetEditingBillId);
  if (!tpl) return;

  // Count historical instances + paid history so the confirm prompt is honest.
  let totalInstances = 0;
  let paidCount = 0;
  for (const yyyymm of Object.keys(billInstances)) {
    for (const key of Object.keys(billInstances[yyyymm])) {
      const inst = billInstances[yyyymm][key];
      if (inst.billId !== _budgetEditingBillId) continue;
      totalInstances++;
      if (inst.paidAt) paidCount++;
    }
  }

  let msg = `Permanently delete "${tpl.name}"?\n\n`;
  if (totalInstances > 0) {
    msg += `This also removes ${totalInstances} bill instance${totalInstances === 1 ? '' : 's'}`;
    if (paidCount > 0) msg += ` (including ${paidCount} marked paid)`;
    msg += `.\n\n`;
  }
  msg += `This cannot be undone. To keep history, archive instead.`;
  if (!confirm(msg)) return;

  // Second confirmation — extra safety for paid bills, since wiping payment
  // history is the kind of thing you might regret on the second click.
  if (paidCount > 0) {
    if (!confirm(`Are you sure? ${paidCount} paid record${paidCount === 1 ? '' : 's'} will be lost forever.`)) return;
  }

  await deleteBillHard(_budgetEditingBillId);
  closeModal('bill-editor-modal');
  _budgetEditingBillId = null;
  toast(`"${tpl.name}" deleted`);
  await renderBudget();
}

// ── Mark all past-due monthly bills as paid (the 1st-of-month sweep) ───────
async function budgetMarkAllStandingPaid() {
  const yyyymm = _budgetViewMonth;
  const today  = new Date().toISOString().slice(0, 10);
  const instances = getMonthInstances(yyyymm);
  const candidates = Object.values(instances).filter(i => !i.paidAt && !i.skipped && i.dueDate <= today);
  if (!candidates.length) { toast('Nothing past-due to mark'); return; }
  // Skip variable bills — they need the prompt
  const fixed = candidates.filter(i => {
    const tpl = bills.find(b => b.id === i.billId);
    return tpl && !tpl.variableAmount;
  });
  if (!fixed.length) { toast('Only variable bills past-due — mark each one individually'); return; }
  if (!confirm(`Mark ${fixed.length} past-due fixed bill${fixed.length > 1 ? 's' : ''} as paid?`)) return;
  for (const inst of fixed) {
    await markBillPaid(yyyymm, inst.billId);
  }
  await renderBudget();
  toast(`Marked ${fixed.length} bill${fixed.length > 1 ? 's' : ''} paid`);
}

// ── Regenerate this month from current templates ───────────────────────────
async function budgetRegenerateMonth() {
  const yyyymm = _budgetViewMonth;
  const instances = getMonthInstances(yyyymm);
  const paidCount = Object.values(instances).filter(i => i.paidAt).length;
  let warning = `Regenerate ${yyyymm} from current bill templates?`;
  if (paidCount > 0) {
    warning += `\n\n${paidCount} paid bill${paidCount > 1 ? 's' : ''} will keep their paid status if they still match a template.`;
  }
  if (!confirm(warning)) return;
  await regenerateMonth(yyyymm);
  await renderBudget();
  toast('Month regenerated');
}

// ── Share permission backfill banner ───────────────────────────────────────
function _maybeShowBudgetBackfillBanner() {
  const banner = document.getElementById('budget-backfill-banner');
  if (!banner) return;
  // Guests don't see this — only the share owner can grant perms
  if (typeof _shareState !== 'undefined' && _shareState) { banner.style.display = 'none'; return; }
  // Run the offer detector — populates window._budgetPermBackfillTargets the first time
  maybeOfferBudgetPermBackfill();
  const targets = (typeof window !== 'undefined' ? window._budgetPermBackfillTargets : null) || [];
  banner.style.display = targets.length ? 'block' : 'none';
}

async function confirmBudgetPermBackfill() {
  const n = await applyBudgetPermBackfill();
  hide('budget-backfill-banner');
  if (typeof window !== 'undefined') window._budgetPermBackfillTargets = [];
  if (n) toast(`Granted Budget access to ${n} share${n > 1 ? 's' : ''}`);
  else   toast('No shares to update');
}

function dismissBudgetPermBackfill() {
  hide('budget-backfill-banner');
  if (typeof window !== 'undefined') window._budgetPermBackfillTargets = [];
}


// ═══════════════════════════════════════════════════════════════════════════
//  BUDGET — Phase 2a Foundations (discretionary spend)
//  Insertion point: in app.js, IMMEDIATELY AFTER the Phase 1 BUDGET UI block
//  (just before the GROCERY LIST section).
//
//  Depends on (defined elsewhere): dbGet, dbPut, _syncQueue, _kvEmailHash,
//  uid, toast, _yyyymm, _yyyymmFromString, _parseYyyymm, _nowIso, _money,
//  saveBudgetLocal (Phase 1 already defines all of these except dbPut).
// ═══════════════════════════════════════════════════════════════════════════

// ── State ──────────────────────────────────────────────────────────────────
function getBudgetCategorySeed() {
  // Returns the seed as templates. UI will let user edit before committing.
  return _BUDGET_CATEGORY_SEED.map(s => ({ ...s }));
}

// ── Category CRUD ──────────────────────────────────────────────────────────
async function createBudgetCategory(input) {
  const cat = {
    id:            'cat_' + (typeof uid === 'function' ? uid() : Date.now() + '_' + Math.random().toString(36).slice(2, 7)),
    name:          (input.name || '').trim() || 'Untitled category',
    monthlyBudget: input.monthlyBudget != null ? Number(input.monthlyBudget) : null,
    weeklyBudget:  input.weeklyBudget  != null ? Number(input.weeklyBudget)  : null,
    budgetCycle:   ['monthly', 'weekly', 'none'].includes(input.budgetCycle) ? input.budgetCycle : 'monthly',
    color:         input.color || _pickCategoryColor(),
    archived:      false,
    createdAt:     _nowIso(),
    updatedAt:     _nowIso(),
  };
  budgetCategories.push(cat);
  await saveBudgetSpendLocal();
  _syncQueue?.enqueue();
  return cat;
}

async function updateBudgetCategory(id, patch) {
  const idx = budgetCategories.findIndex(c => c.id === id);
  if (idx === -1) return null;
  budgetCategories[idx] = { ...budgetCategories[idx], ...patch, updatedAt: _nowIso() };
  await saveBudgetSpendLocal();
  _syncQueue?.enqueue();
  return budgetCategories[idx];
}

async function archiveBudgetCategory(id) {
  return updateBudgetCategory(id, { archived: true });
}

async function unarchiveBudgetCategory(id) {
  return updateBudgetCategory(id, { archived: false });
}

// Hard delete — only used when user is sure (e.g. category has no history)
// Adds tombstone so sync doesn't resurrect.
async function deleteBudgetCategoryHard(id) {
  budgetCategories = budgetCategories.filter(c => c.id !== id);
  budgetCategoryDeletedIds.add(id);
  // Optionally orphan any transactions referencing it — by leaving categoryId
  // pointing at a non-existent category. UI can render these as "Uncategorised".
  await saveBudgetSpendLocal();
  _syncQueue?.enqueue();
}

function _pickCategoryColor() {
  const used = new Set(budgetCategories.map(c => c.color));
  for (const c of _BUDGET_PALETTE) if (!used.has(c)) return c;
  return _BUDGET_PALETTE[budgetCategories.length % _BUDGET_PALETTE.length];
}

// ── Transaction CRUD ───────────────────────────────────────────────────────
async function updateTransaction(id, patch) {
  // Tx might have moved months if date changed — find current location first
  const located = _findTransaction(id);
  if (!located) return null;
  const { yyyymm: oldYm, tx: existing } = located;
  const merged = { ...existing, ...patch, updatedAt: _nowIso() };
  const newYm = patch.date ? _yyyymmFromString(patch.date) : oldYm;

  if (newYm !== oldYm) {
    // Move across months
    delete transactions[oldYm][id];
    if (Object.keys(transactions[oldYm]).length === 0) delete transactions[oldYm];
    if (!transactions[newYm]) transactions[newYm] = {};
    transactions[newYm][id] = merged;
  } else {
    transactions[oldYm][id] = merged;
  }
  await saveBudgetSpendLocal();
  _syncQueue?.enqueue();
  return merged;
}

async function deleteTransaction(id) {
  const located = _findTransaction(id);
  if (!located) return false;
  delete transactions[located.yyyymm][id];
  if (Object.keys(transactions[located.yyyymm]).length === 0) {
    delete transactions[located.yyyymm];
  }
  budgetTransactionDeletedIds.add(id);
  await saveBudgetSpendLocal();
  _syncQueue?.enqueue();
  return true;
}

function _findTransaction(id) {
  for (const yyyymm of Object.keys(transactions)) {
    if (transactions[yyyymm][id]) return { yyyymm, tx: transactions[yyyymm][id] };
  }
  return null;
}

function getTransaction(id) {
  return _findTransaction(id)?.tx || null;
}

function getTransactionsForMonth(yyyymm) {
  return Object.values(transactions[yyyymm] || {});
}

function getTransactionsForRange(startIso, endIso) {
  // startIso and endIso are 'YYYY-MM-DD' inclusive
  const out = [];
  const startMonth = _yyyymmFromString(startIso);
  const endMonth   = _yyyymmFromString(endIso);
  for (const yyyymm of Object.keys(transactions)) {
    if (yyyymm < startMonth || yyyymm > endMonth) continue;
    for (const tx of Object.values(transactions[yyyymm])) {
      if (tx.date >= startIso && tx.date <= endIso) out.push(tx);
    }
  }
  return out;
}

// ── Aggregations ───────────────────────────────────────────────────────────
function getSpendForCategory(yyyymm, categoryId) {
  let total = 0;
  for (const tx of Object.values(transactions[yyyymm] || {})) {
    if (tx.categoryId === categoryId) total += (tx.amount || 0);
  }
  return Math.round(total * 100) / 100;
}

function getSpendForCategoryInRange(startIso, endIso, categoryId) {
  let total = 0;
  for (const tx of getTransactionsForRange(startIso, endIso)) {
    if (tx.categoryId === categoryId) total += (tx.amount || 0);
  }
  return Math.round(total * 100) / 100;
}

function getCategoryProgress(categoryId, period, referenceDate = null, weekStart = 'mon') {
  const cat = getBudgetCategoryById(categoryId);
  if (!cat) return null;

  let spent, budget;
  if (period === 'week') {
    const ref = referenceDate ? new Date(referenceDate + 'T12:00:00') : new Date();
    const { startIso, endIso } = getWeekRange(ref, weekStart);
    spent  = getSpendForCategoryInRange(startIso, endIso, categoryId);
    budget = (cat.budgetCycle === 'weekly') ? cat.weeklyBudget
           : (cat.monthlyBudget != null ? cat.monthlyBudget / 4.345 : null);
  } else {
    const yyyymm = referenceDate ? _yyyymmFromString(referenceDate) : _yyyymm(new Date());
    spent  = getSpendForCategory(yyyymm, categoryId);
    budget = (cat.budgetCycle === 'monthly') ? cat.monthlyBudget
           : (cat.weeklyBudget != null ? cat.weeklyBudget * 4.345 : null);
  }

  if (budget == null || budget === 0) {
    return { spent: Math.round(spent * 100) / 100, budget: null, pct: null, status: 'none' };
  }
  const pct = spent / budget;
  let status = 'ok';
  if      (pct >= 1.0)  status = 'over';
  else if (pct >= 0.8)  status = 'warn';
  return {
    spent:  Math.round(spent * 100) / 100,
    budget: Math.round(budget * 100) / 100,
    pct,
    status,
  };
}

// ── Period helpers ─────────────────────────────────────────────────────────
// Returns the start and end ISO dates of the week containing `date`,
// where weekStart is 'mon' or 'sun'.
function getWeekRange(date, weekStart = 'mon') {
  const d = new Date(date);
  d.setHours(12, 0, 0, 0);
  const dow = d.getDay(); // 0=Sun..6=Sat
  const offsetToStart = (weekStart === 'mon')
    ? (dow === 0 ? -6 : 1 - dow)   // Mon=1; Sunday wraps to -6
    : (-dow);                       // Sun=0
  const start = new Date(d);
  start.setDate(d.getDate() + offsetToStart);
  const end = new Date(start);
  end.setDate(start.getDate() + 6);
  const iso = (x) => `${x.getFullYear()}-${String(x.getMonth()+1).padStart(2,'0')}-${String(x.getDate()).padStart(2,'0')}`;
  return { startIso: iso(start), endIso: iso(end), startDate: start, endDate: end };
}

// ── Quick-add parser ───────────────────────────────────────────────────────
// Parses input like:
//   "tesco 47.50, m&s 12.99 shopping, parking 4 monday-nights"
// Returns an array of partially-parsed entries:
//   [{ where, amount, categoryHint, categoryId, raw }, ...]
// `categoryId` is resolved using merchant memory + alias matching + fallback.
//
// Tokenisation rules:
//   - Comma-separated entries.
//   - Within an entry, words are separated by whitespace.
//   - Exactly one word is the amount (a number, optionally with £ or decimals).
//   - The last non-amount word MAY be a category alias/name. If it matches one,
//     it's the category hint. Otherwise it's part of the merchant name.
//   - The remaining words form the merchant ('where').
//
// Resolution order for category:
//   1. Explicit hint matched against alias map → use that category.
//   2. Merchant memory: look for the most-recent transaction with matching
//      `where` (case-insensitive) and use its categoryId.
//   3. Fallback to `defaultCategoryId` (passed by caller).
//
// Returns null entries for un-parseable tokens — caller decides what to show.
let _spendPeriod          = 'week';     // 'week' | 'month'
let _spendReferenceDate   = null;       // ISO date for the week we're viewing; null = today
let _spendEditingTxId     = null;
let _spendEditingCatId    = null;
function renderBudgetSpend() {
  const cats = getActiveBudgetCategories();
  const empty = document.getElementById('budget-spend-empty');
  const content = document.getElementById('budget-spend-content');

  if (cats.length === 0) {
    if (empty)   empty.style.display   = 'block';
    if (content) content.style.display = 'none';
    return;
  }
  if (empty)   empty.style.display   = 'none';
  if (content) content.style.display = 'block';

  _renderSpendHeader();
  _renderSpendCategoryTiles();
  _renderSpendTransactionList();
}

function _spendCurrentRange() {
  const today = new Date();
  if (_spendPeriod === 'month') {
    const ref = _spendReferenceDate ? new Date(_spendReferenceDate + 'T12:00:00') : today;
    const y = ref.getFullYear(), m = ref.getMonth();
    const startIso = `${y}-${String(m+1).padStart(2,'0')}-01`;
    const endDate  = new Date(y, m + 1, 0);
    const endIso   = `${y}-${String(m+1).padStart(2,'0')}-${String(endDate.getDate()).padStart(2,'0')}`;
    return { startIso, endIso, label: ref.toLocaleDateString(undefined, { month: 'long', year: 'numeric' }) };
  }
  // week
  const ref = _spendReferenceDate ? new Date(_spendReferenceDate + 'T12:00:00') : today;
  const wk  = getWeekRange(ref, budgetSettings.weekStart || 'mon');
  const fmt = (d) => d.toLocaleDateString(undefined, { day: 'numeric', month: 'short' });
  return { startIso: wk.startIso, endIso: wk.endIso, label: `${fmt(wk.startDate)} – ${fmt(wk.endDate)}` };
}

function _renderSpendHeader() {
  const { startIso, endIso, label } = _spendCurrentRange();
  const labelEl = document.getElementById('budget-spend-period-label');
  const totalEl = document.getElementById('budget-spend-total');
  if (labelEl) labelEl.textContent = label;

  // Total = sum of transactions in current range
  const txs = getTransactionsForRange(startIso, endIso);
  const total = txs.reduce((s, t) => s + (t.amount || 0), 0);
  if (totalEl) totalEl.textContent = _money(total);

  // Period toggle visual
  document.querySelectorAll('.budget-period-btn').forEach(btn => {
    const active = btn.dataset.period === _spendPeriod;
    btn.classList.toggle('active', active);
    btn.style.background = active ? 'var(--surface)' : 'transparent';
    btn.style.color      = active ? 'var(--text)'    : 'var(--muted)';
  });

  // "Today" button visibility
  const todayBtn = document.getElementById('budget-spend-today');
  if (todayBtn) {
    const today = new Date().toISOString().slice(0, 10);
    const onToday = (today >= startIso && today <= endIso);
    todayBtn.style.opacity = onToday ? '0.4' : '1';
  }
}

function _renderSpendCategoryTiles() {
  const host = document.getElementById('budget-spend-tiles');
  if (!host) return;
  const { startIso, endIso } = _spendCurrentRange();

  const cats = getActiveBudgetCategories();
  if (!cats.length) { host.innerHTML = ''; return; }

  host.innerHTML = cats.map(cat => _renderCategoryTile(cat, startIso, endIso, _spendPeriod, true)).join('');
}

function _renderCategoryTile(cat, startIso, endIso, period, clickable) {
  const spent = getSpendForCategoryInRange(startIso, endIso, cat.id);
  let budget = null;
  if (period === 'week') {
    budget = (cat.budgetCycle === 'weekly') ? cat.weeklyBudget
           : (cat.monthlyBudget != null ? cat.monthlyBudget / 4.345 : null);
  } else {
    budget = (cat.budgetCycle === 'monthly') ? cat.monthlyBudget
           : (cat.weeklyBudget != null ? cat.weeklyBudget * 4.345 : null);
  }
  const pct = (budget && budget > 0) ? Math.min(spent / budget, 1.5) : 0;
  let barClass = 'is-ok';
  if (budget && spent / budget >= 1)    barClass = 'is-over';
  else if (budget && spent / budget >= 0.8) barClass = 'is-warn';
  const isFiltered = (_spendCategoryFilter === cat.id);

  const budgetText = (budget != null && budget > 0)
    ? `<span style="color:var(--muted);font-weight:400">/ ${_money(budget)}</span>`
    : '';
  const barWidth = budget ? Math.min(pct * 100, 100) : 0;

  // Remaining line: budget − spent, framed as a result the eye can grab
  // instantly. Green when within budget, red (negative) when over. Hidden
  // entirely for categories with no budget set.
  let remainingText = '';
  if (budget != null && budget > 0) {
    const remaining = budget - spent;
    if (remaining >= 0) {
      remainingText = `<div class="budget-cat-tile-left is-ok">${_money(remaining)} <span class="budget-cat-tile-left-lbl">left</span></div>`;
    } else {
      remainingText = `<div class="budget-cat-tile-left is-over">−${_money(Math.abs(remaining))} <span class="budget-cat-tile-left-lbl">over</span></div>`;
    }
  }

  return `
    <div class="budget-cat-tile ${isFiltered ? 'is-filtered' : ''}${clickable && bulkSelectionHas('category', cat.id) ? ' selected' : ''}"
         ${clickable ? `data-bulk-id="${cat.id}" data-bulk-section="category" onclick="onCategoryTileClick(event,'${cat.id}')"` : ''}>
      <div class="budget-cat-tile-name" style="color:${cat.color || 'var(--text)'}">
        <span class="budget-cat-dot" style="background:${cat.color || 'var(--accent)'}"></span>
        ${_escapeHtml(cat.name)}${(isOwner() && cat.share != null) ? (() => {
          // Per-category sharing override indicator (owner-only). Same
          // pattern as the item/list/reminder card indicators.
          const sh = cat.share;
          let icon = 'i-shield', title = 'Custom sharing';
          if (sh === 'private') { icon = 'i-eye-off'; title = 'Private — owner only'; }
          else if (typeof sh === 'object') {
            if (Array.isArray(sh.allow))     title = `Visible to ${sh.allow.length} share${sh.allow.length===1?'':'s'} only`;
            else if (Array.isArray(sh.deny)) title = `Hidden from ${sh.deny.length} share${sh.deny.length===1?'':'s'}`;
            if (Array.isArray(sh.readOnly) && sh.readOnly.length) title += ` · read-only for ${sh.readOnly.length}`;
          }
          return ` <svg class="icon icon-sm" aria-hidden="true" title="${_escapeHtml(title)}" style="color:var(--muted);vertical-align:-2px"><use href="#${icon}"></use></svg>`;
        })() : ''}
      </div>
      <div class="budget-cat-tile-amt">${_money(spent)} ${budgetText}</div>
      ${remainingText}
      <div class="budget-cat-tile-bar"><div class="budget-cat-tile-bar-fill ${barClass}" style="width:${barWidth}%;background:${cat.color || 'var(--accent)'}"></div></div>
    </div>`;
}

// Click handler for spend-view category tiles. In bulk mode, toggles
// selection. Otherwise routes to the existing filter toggle as before.
function onCategoryTileClick(event, id) {
  if (isBulkSelectMode('category')) {
    if (event && event.stopPropagation) event.stopPropagation();
    toggleBulkSelection('category', id);
    return;
  }
  toggleSpendCategoryFilter(id);
}

function toggleSpendCategoryFilter(catId) {
  _spendCategoryFilter = (_spendCategoryFilter === catId) ? null : catId;
  _renderSpendCategoryTiles();
  _renderSpendTransactionList();
}

function _renderSpendTransactionList() {
  const host = document.getElementById('budget-spend-tx-list');
  if (!host) return;

  const { startIso, endIso } = _spendCurrentRange();
  let txs = getTransactionsForRange(startIso, endIso);
  if (_spendCategoryFilter) {
    txs = txs.filter(t => t.categoryId === _spendCategoryFilter);
  }

  if (txs.length === 0) {
    const filterMsg = _spendCategoryFilter
      ? 'No transactions for this category in this period'
      : 'No transactions in this period';
    host.innerHTML = `<div style="padding:30px 16px;text-align:center;color:var(--muted);font-size:13px">${filterMsg}</div>`;
    return;
  }

  // Group by date
  txs.sort((a, b) => {
    const c = b.date.localeCompare(a.date);
    if (c !== 0) return c;
    return (b.createdAt || '').localeCompare(a.createdAt || '');
  });

  const groups = {};
  for (const tx of txs) {
    if (!groups[tx.date]) groups[tx.date] = [];
    groups[tx.date].push(tx);
  }

  const dateKeys = Object.keys(groups).sort((a, b) => b.localeCompare(a));
  host.innerHTML = dateKeys.map(date => {
    const dayLabel = _spendDayHeading(date);
    const dayTotal = groups[date].reduce((s, t) => s + (t.amount || 0), 0);
    return `
      <div class="spend-day-group">
        <div class="spend-day-heading">
          <span>${dayLabel}</span>
          <span style="color:var(--muted);font-family:var(--mono);font-size:11px">${_money(dayTotal)}</span>
        </div>
        ${groups[date].map(_renderTransactionRow).join('')}
      </div>`;
  }).join('');
}

function _spendDayHeading(iso) {
  const d = new Date(iso + 'T12:00:00');
  const today = new Date(); today.setHours(0,0,0,0);
  const dDate = new Date(iso + 'T00:00:00');
  const days = Math.round((today - dDate) / MS_PER_DAY);
  if (days === 0)  return 'Today, ' + d.toLocaleDateString(undefined, { day:'numeric', month:'short' });
  if (days === 1)  return 'Yesterday, ' + d.toLocaleDateString(undefined, { day:'numeric', month:'short' });
  return d.toLocaleDateString(undefined, { weekday:'short', day:'numeric', month:'short' });
}

function _renderTransactionRow(tx) {
  const cat  = getBudgetCategoryById(tx.categoryId);
  const catName = cat ? cat.name : 'Uncategorised';
  const catColor = cat ? cat.color : 'var(--muted)';
  const where = tx.where || '(no merchant)';
  // Per-tx sharing indicator + bulk-select wiring removed in Pass 2d-
  // rollback. The unit of share is now the CATEGORY (Pass 2e). A
  // per-tx click just opens the editor directly.
  return `
    <div class="spend-tx-row" onclick="openSpendTxEditor('${tx.id}')">
      <div class="spend-tx-info">
        <div class="spend-tx-where">${_escapeHtml(where)}</div>
        <div class="spend-tx-cat"><span class="budget-cat-dot" style="background:${catColor}"></span>${_escapeHtml(catName)}</div>
      </div>
      <div class="spend-tx-amount">${_money(tx.amount)}</div>
    </div>`;
}

// onSpendTxRowClick removed in Pass 2d-rollback — the row's onclick
// goes straight to openSpendTxEditor since transactions are no longer
// a bulk-selectable unit.

// ── Period navigation ──────────────────────────────────────────────────────
function spendSwitchPeriod(period) {
  if (period !== 'week' && period !== 'month') return;
  _spendPeriod = period;
  // Reset reference date so the new period always anchors on today.
  // Without this, switching week→month while viewing a stale ref week could
  // land you on the wrong calendar month and hide today's transactions.
  _spendReferenceDate = null;
  renderBudgetSpend();
}

function spendPrevPeriod() {
  const ref = _spendReferenceDate ? new Date(_spendReferenceDate + 'T12:00:00') : new Date();
  if (_spendPeriod === 'week') {
    ref.setDate(ref.getDate() - 7);
  } else {
    ref.setMonth(ref.getMonth() - 1);
  }
  _spendReferenceDate = ref.toISOString().slice(0, 10);
  renderBudgetSpend();
}

function spendNextPeriod() {
  const ref = _spendReferenceDate ? new Date(_spendReferenceDate + 'T12:00:00') : new Date();
  if (_spendPeriod === 'week') {
    ref.setDate(ref.getDate() + 7);
  } else {
    ref.setMonth(ref.getMonth() + 1);
  }
  _spendReferenceDate = ref.toISOString().slice(0, 10);
  renderBudgetSpend();
}

function spendGoToday() {
  _spendReferenceDate = null;
  renderBudgetSpend();
}

// ── Quick Add modal ────────────────────────────────────────────────────────
function openSpendTxEditor(txId) {
  const tx = getTransaction(txId);
  if (!tx) return;
  _spendEditingTxId = txId;
  document.getElementById('spend-tx-where').value  = tx.where  || '';
  document.getElementById('spend-tx-amount').value = tx.amount ?? '';
  document.getElementById('spend-tx-date').value   = tx.date   || '';
  document.getElementById('spend-tx-notes').value  = tx.notes  || '';

  // Populate categories
  const sel = document.getElementById('spend-tx-category');
  sel.innerHTML = '<option value="">— Uncategorised —</option>' +
    getActiveBudgetCategories().map(c =>
      `<option value="${c.id}" ${c.id === tx.categoryId ? 'selected' : ''}>${_escapeHtml(c.name)}</option>`
    ).join('');

  openModal('spend-tx-modal');
  setTimeout(() => document.getElementById('spend-tx-where').focus(), 50);
  // openSharingPanelFor('transaction') call removed in Pass 2d-rollback.
  // Per-tx sharing is gone; visibility flows from the tx's category.
}

async function saveSpendTxFromEditor() {
  if (!_spendEditingTxId) return;
  const where  = (document.getElementById('spend-tx-where').value || '').trim();
  const amount = parseFloat(document.getElementById('spend-tx-amount').value);
  if (isNaN(amount) || amount <= 0) { toast('Enter a valid amount'); return; }
  const date = document.getElementById('spend-tx-date').value;
  if (!date)   { toast('Pick a date'); return; }
  const notes = (document.getElementById('spend-tx-notes').value || '').trim();
  const categoryId = document.getElementById('spend-tx-category').value || null;

  await updateTransaction(_spendEditingTxId, { where, amount, date, notes, categoryId });
  closeModal('spend-tx-modal');
  _spendEditingTxId = null;
  toast('Transaction updated');
  if (_currentView === 'budget') {
    if (_budgetActivePanel === 'spend') renderBudgetSpend();
    else if (_budgetActivePanel === 'dashboard') renderBudgetDashboard();
  }
}

async function confirmDeleteSpendTx() {
  if (!_spendEditingTxId) return;
  if (!confirm('Delete this transaction? This cannot be undone.')) return;
  await deleteTransaction(_spendEditingTxId);
  closeModal('spend-tx-modal');
  _spendEditingTxId = null;
  toast('Transaction deleted');
  if (_currentView === 'budget') {
    if (_budgetActivePanel === 'spend') renderBudgetSpend();
    else if (_budgetActivePanel === 'dashboard') renderBudgetDashboard();
  }
}

// ── Category management modal ──────────────────────────────────────────────
function openManageBudgetCategories() {
  _renderBudgetCategoryList();
  openModal('budget-cat-modal');
}

function _renderBudgetCategoryList() {
  const activeHost   = document.getElementById('budget-cat-active-list');
  const archivedHost = document.getElementById('budget-cat-archived-list');
  const archivedSection = document.getElementById('budget-cat-archived-section');

  const active   = budgetCategories.filter(c => !c.archived);
  const archived = budgetCategories.filter(c =>  c.archived);

  if (active.length === 0) {
    activeHost.innerHTML = '<div style="padding:14px;text-align:center;color:var(--muted);font-size:12px">No categories yet</div>';
  } else {
    activeHost.innerHTML = active.map(_renderBudgetCategoryRow).join('');
  }

  if (archived.length === 0) {
    archivedSection.style.display = 'none';
  } else {
    archivedSection.style.display = '';
    archivedHost.innerHTML = archived.map(_renderBudgetCategoryRow).join('');
  }
}

function _renderBudgetCategoryRow(cat) {
  const cycleLabel = cat.budgetCycle === 'weekly'  ? 'weekly'
                   : cat.budgetCycle === 'monthly' ? 'monthly'
                   : 'no budget';
  const budgetText = (cat.budgetCycle === 'weekly' && cat.weeklyBudget != null)  ? `${_money(cat.weeklyBudget)} / wk`
                   : (cat.budgetCycle === 'monthly' && cat.monthlyBudget != null) ? `${_money(cat.monthlyBudget)} / mo`
                   : 'no budget';
  const archivedActions = `
    <button class="bill-action-btn" onclick="event.stopPropagation();handleUnarchiveBudgetCategory('${cat.id}')" title="Unarchive">
      <svg aria-hidden="true"><use href="#i-refresh-cw"></use></svg>
    </button>`;
  const activeActions = `
    <button class="bill-action-btn" onclick="event.stopPropagation();openBudgetCategoryEditor('${cat.id}')" title="Edit">
      <svg aria-hidden="true"><use href="#i-pencil"></use></svg>
    </button>`;
  return `
    <div class="bill-row budget-cat-row ${cat.archived ? 'is-skipped' : ''}${bulkSelectionHas('category', cat.id) ? ' selected' : ''}" data-bulk-id="${cat.id}" data-bulk-section="category" onclick="onCategoryRowClick(event,'${cat.id}')" style="cursor:pointer">
      <div class="bill-day" style="background:${cat.color || 'var(--surface)'};color:#000;border-color:${cat.color || 'var(--border)'}">●</div>
      <div class="bill-info">
        <div class="bill-name">${_escapeHtml(cat.name)}${(isOwner() && cat.share != null) ? (() => {
          // Per-category sharing override indicator (owner-only, same as
          // the spend-view tile indicator).
          const sh = cat.share;
          let icon = 'i-shield', title = 'Custom sharing';
          if (sh === 'private') { icon = 'i-eye-off'; title = 'Private — owner only'; }
          else if (typeof sh === 'object') {
            if (Array.isArray(sh.allow))     title = `Visible to ${sh.allow.length} share${sh.allow.length===1?'':'s'} only`;
            else if (Array.isArray(sh.deny)) title = `Hidden from ${sh.deny.length} share${sh.deny.length===1?'':'s'}`;
            if (Array.isArray(sh.readOnly) && sh.readOnly.length) title += ` · read-only for ${sh.readOnly.length}`;
          }
          return ` <svg class="icon icon-sm" aria-hidden="true" title="${_escapeHtml(title)}" style="color:var(--muted);vertical-align:-2px"><use href="#${icon}"></use></svg>`;
        })() : ''}</div>
        <div class="bill-meta">${budgetText}</div>
      </div>
      <div class="bill-actions">
        ${cat.archived ? archivedActions : activeActions}
      </div>
    </div>`;
}

// Click handler for Manage Categories rows. In bulk mode, toggles
// selection. Otherwise opens the editor as before.
function onCategoryRowClick(event, id) {
  if (isBulkSelectMode('category')) {
    if (event && event.stopPropagation) event.stopPropagation();
    toggleBulkSelection('category', id);
    return;
  }
  openBudgetCategoryEditor(id);
}

async function handleUnarchiveBudgetCategory(id) {
  await unarchiveBudgetCategory(id);
  _renderBudgetCategoryList();
  if (_currentView === 'budget') renderBudget();
}

// ── Budget category sharing-panel registration (Pass 2e-b) ────────────
// Single-record sharing UI inside the category editor. Reuses the
// generic _renderSharingPanel module — same UX as item/list/reminder
// sharing. _spendEditingCatId is the natural currentId source. save()
// hits saveBudgetSpendLocal so the share field is persisted alongside
// other category fields.
registerSharingSection('category', {
  findRecord: (id) => getBudgetCategoryById(id),
  currentId:  ()   => _spendEditingCatId,
  save:       ()   => saveBudgetSpendLocal(),
  mountSectionEl: () => document.getElementById('bcat-sharing-section'),
  mountContentEl: () => document.getElementById('bcat-sharing-content'),
  noun: 'category',
});

// ── Budget category bulk-select registration (Pass 2e-c) ──────────────
// Two entry points: a Select button on the Spend screen (above the tile
// row) AND on the Manage Categories list. Same selection set across
// both — user can switch screens while in select mode and what they
// picked persists.
//
// NO Delete button — hard-deleting categories is dangerous when bulk-
// applied (would lose spend history references). The path stays: edit
// → Archive (soft), then in the archived list per-row Delete forever
// for cases where the user is sure. The bulk module hides the Delete
// button automatically when applyDelete is omitted.
//
// Archive IS supported in bulk — that's the safe, reversible action.
//
// getVisibleIds queries BOTH renders (spend tiles AND manage-categories
// rows). Whichever screen is open contributes ids; the other returns
// empty. If both are somehow visible (a future overlay scenario), de-
// duping via Set takes care of double-counts.
registerBulkSelectSection('category', {
  findRecord: (id) => getBudgetCategoryById(id),
  save:       ()   => saveBudgetSpendLocal(),
  rerender:   ()   => {
    // Re-render the budget tab if we're on it, AND the manage-categories
    // list if it's mounted. Both could be visible at once (manage opens
    // as a modal over the budget view). Cheap to do both.
    if (_currentView === 'budget') renderBudget();
    _renderBudgetCategoryList();
  },
  getVisibleIds: () => {
    const ids = new Set();
    document.querySelectorAll('[data-bulk-id][data-bulk-section="category"]')
      .forEach(el => {
        const id = el.getAttribute('data-bulk-id');
        if (id) ids.add(id);
      });
    return [...ids];
  },
  permCheck:  ()   => {
    if (!canWrite('budget')) { showLockBanner('budget'); return false; }
    return true;
  },
  applyArchive: (cat) => {
    cat.archived  = true;
    cat.updatedAt = _nowIso();
  },
  // NO applyDelete — bulk Delete button is hidden by the module.
  // Hard-delete stays per-category via the editor's "Delete forever"
  // button on already-archived categories.
  sectionPermKey: 'budget',
  noun:           'category',
  pluralNoun:     'categories',
});

function openBudgetCategoryEditor(catId = null) {
  _spendEditingCatId = catId;
  const cat = catId ? getBudgetCategoryById(catId) : null;
  document.getElementById('budget-cat-editor-mode-label').textContent = cat ? 'Edit Category' : 'Add Category';
  document.getElementById('budget-cat-editor-save-label').textContent = cat ? 'Save' : 'Create';
  toggle('budget-cat-archive-btn', cat && !cat.archived, 'inline-flex');
  toggle('budget-cat-delete-btn', cat &&  cat.archived, 'inline-flex');

  document.getElementById('budget-cat-name').value         = cat?.name           || '';
  document.getElementById('budget-cat-cycle').value        = cat?.budgetCycle    || 'monthly';
  document.getElementById('budget-cat-monthly').value      = cat?.monthlyBudget  ?? '';
  document.getElementById('budget-cat-weekly').value       = cat?.weeklyBudget   ?? '';
  document.getElementById('budget-cat-color').value        = cat?.color          || '#5b8dee';

  budgetCatCycleChanged();
  openModal('budget-cat-editor-modal');
  setTimeout(() => document.getElementById('budget-cat-name').focus(), 50);
  // Render the sharing panel for this category. Hidden by the module
  // when adding a new cat (no record yet), when not the owner, or when
  // no shares are configured.
  if (cat) {
    openSharingPanelFor('category');
  } else {
    // New-category flow: explicitly hide so a stale panel from a prior
    // edit session doesn't leak through.
    const _bcs = document.getElementById('bcat-sharing-section');
    if (_bcs) _bcs.style.display = 'none';
  }
}

function budgetCatCycleChanged() {
  const cycle = document.getElementById('budget-cat-cycle').value;
  toggle('budget-cat-monthly-row', (cycle === 'monthly'), 'block');
  toggle('budget-cat-weekly-row', (cycle === 'weekly'), 'block');
}

async function saveBudgetCategoryFromEditor() {
  const name = (document.getElementById('budget-cat-name').value || '').trim();
  if (!name) { toast('Category needs a name'); return; }
  const cycle = document.getElementById('budget-cat-cycle').value;
  const monthlyVal = document.getElementById('budget-cat-monthly').value;
  const weeklyVal  = document.getElementById('budget-cat-weekly').value;
  const monthlyBudget = (cycle === 'monthly' && monthlyVal !== '') ? Number(monthlyVal) : null;
  const weeklyBudget  = (cycle === 'weekly'  && weeklyVal  !== '') ? Number(weeklyVal)  : null;
  const color = document.getElementById('budget-cat-color').value || '#5b8dee';

  const patch = { name, budgetCycle: cycle, monthlyBudget, weeklyBudget, color };

  if (_spendEditingCatId) {
    await updateBudgetCategory(_spendEditingCatId, patch);
    toast('Category updated');
  } else {
    await createBudgetCategory(patch);
    toast('Category added');
  }

  closeModal('budget-cat-editor-modal');
  _spendEditingCatId = null;
  _renderBudgetCategoryList();
  if (_currentView === 'budget') renderBudget();
}

async function confirmArchiveBudgetCategory() {
  if (!_spendEditingCatId) return;
  const cat = getBudgetCategoryById(_spendEditingCatId);
  if (!cat) return;
  if (!confirm(`Archive "${cat.name}"? Past transactions stay; the category won't appear in tiles or quick-add.`)) return;
  await archiveBudgetCategory(_spendEditingCatId);
  closeModal('budget-cat-editor-modal');
  _spendEditingCatId = null;
  toast('Category archived');
  _renderBudgetCategoryList();
  if (_currentView === 'budget') renderBudget();
}

async function confirmDeleteBudgetCategory() {
  if (!_spendEditingCatId) return;
  const cat = getBudgetCategoryById(_spendEditingCatId);
  if (!cat) return;
  if (!confirm(`Permanently delete "${cat.name}"? Past transactions remain but become uncategorised. This can't be undone.`)) return;
  await deleteBudgetCategoryHard(_spendEditingCatId);
  closeModal('budget-cat-editor-modal');
  _spendEditingCatId = null;
  toast('Category deleted');
  _renderBudgetCategoryList();
  if (_currentView === 'budget') renderBudget();
}

// ── First-run setup modal ──────────────────────────────────────────────────
function openBudgetSetupModal() {
  const seed = getBudgetCategorySeed();
  const list = document.getElementById('budget-setup-list');
  list.innerHTML = seed.map((s, i) => `
    <div class="budget-setup-row">
      <input type="checkbox" id="setup-row-${i}-on" checked>
      <input type="text" id="setup-row-${i}-name" value="${_escapeHtml(s.name)}" maxlength="40"
        style="flex:2;background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:7px 10px;color:var(--text);font-family:var(--sans);font-size:13px">
      <span style="color:var(--muted);font-size:12px">${currencySymbol()}</span>
      <input type="number" id="setup-row-${i}-amount" value="${s.monthlyBudget}" min="0" step="1"
        style="width:80px;background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:7px 10px;color:var(--text);font-family:var(--mono);font-size:13px">
      <span style="color:var(--muted);font-size:11px">/ mo</span>
    </div>`).join('');
  openModal('budget-setup-modal');
}

async function confirmBudgetSetup() {
  const seed = getBudgetCategorySeed();
  let created = 0;
  for (let i = 0; i < seed.length; i++) {
    const enabled = document.getElementById(`setup-row-${i}-on`).checked;
    if (!enabled) continue;
    const name = (document.getElementById(`setup-row-${i}-name`).value || '').trim();
    if (!name) continue;
    const amount = parseFloat(document.getElementById(`setup-row-${i}-amount`).value);
    await createBudgetCategory({
      name,
      monthlyBudget: isNaN(amount) ? null : amount,
      weeklyBudget:  null,
      budgetCycle:   'monthly',
      color:         seed[i].color,
    });
    created++;
  }
  _budgetSetupOffered = true;
  await saveBudgetSpendLocal();
  closeModal('budget-setup-modal');
  if (created) toast(`Added ${created} categor${created === 1 ? 'y' : 'ies'}`);
  if (_currentView === 'budget') renderBudget();
}

async function dismissBudgetSetup() {
  _budgetSetupOffered = true;
  await saveBudgetSpendLocal();
  closeModal('budget-setup-modal');
}

// ── Settings: week-start radio handler ─────────────────────────────────────
async function setBudgetWeekStartFromSettings(value) {
  await setBudgetWeekStart(value);
  // If the spend panel is open, re-render so week boundaries shift
  if (_currentView === 'budget' && _budgetActivePanel === 'spend') {
    renderBudgetSpend();
  }
}

async function createAccount(input) {
  const today = new Date().toISOString().slice(0, 10);
  const acc = {
    id:           'acc_' + (typeof uid === 'function' ? uid() : Date.now() + '_' + Math.random().toString(36).slice(2, 7)),
    name:         (input.name || '').trim() || 'Untitled account',
    type:         ['current', 'savings', 'credit_card'].includes(input.type) ? input.type : 'current',
    isPrimary:    !!input.isPrimary,
    balance:      input.balance != null ? Number(input.balance) : 0,
    balanceAsOf:  input.balanceAsOf || today,
    color:        input.color || _pickAccountColor(),
    notes:        input.notes || '',
    archived:     false,
    createdAt:    _nowIso(),
    updatedAt:    _nowIso(),
  };
  // If marking primary, unset any other primary first
  if (acc.isPrimary) {
    for (const a of budgetAccounts) {
      if (a.isPrimary && !a.archived) {
        a.isPrimary = false;
        a.updatedAt = _nowIso();
      }
    }
  }
  // If this is the first non-archived account, make it primary by default
  const otherActive = budgetAccounts.filter(a => !a.archived);
  if (!otherActive.length && !acc.isPrimary) acc.isPrimary = true;

  budgetAccounts.push(acc);
  await saveBudgetAccountsAndIncomeLocal();
  _syncQueue?.enqueue();
  return acc;
}

async function updateAccount(id, patch) {
  const idx = budgetAccounts.findIndex(a => a.id === id);
  if (idx === -1) return null;
  // If setting isPrimary=true, unset others
  if (patch.isPrimary === true) {
    for (const a of budgetAccounts) {
      if (a.id !== id && a.isPrimary && !a.archived) {
        a.isPrimary = false;
        a.updatedAt = _nowIso();
      }
    }
  }
  budgetAccounts[idx] = { ...budgetAccounts[idx], ...patch, updatedAt: _nowIso() };
  await saveBudgetAccountsAndIncomeLocal();
  _syncQueue?.enqueue();
  return budgetAccounts[idx];
}

async function archiveAccount(id) {
  const acc = budgetAccounts.find(a => a.id === id);
  if (!acc) return null;
  // If archiving the primary, demote it and elect a new primary if any other active accounts
  if (acc.isPrimary) {
    const others = budgetAccounts.filter(a => a.id !== id && !a.archived);
    if (others.length) {
      others[0].isPrimary = true;
      others[0].updatedAt = _nowIso();
    }
  }
  return updateAccount(id, { archived: true, isPrimary: false });
}

async function unarchiveAccount(id) {
  return updateAccount(id, { archived: false });
}

async function deleteAccountHard(id) {
  budgetAccounts = budgetAccounts.filter(a => a.id !== id);
  budgetAccountDeletedIds.add(id);
  // Promote a new primary if needed
  const anyPrimary = budgetAccounts.some(a => !a.archived && a.isPrimary);
  if (!anyPrimary) {
    const candidate = budgetAccounts.find(a => !a.archived);
    if (candidate) {
      candidate.isPrimary = true;
      candidate.updatedAt = _nowIso();
    }
  }
  await saveBudgetAccountsAndIncomeLocal();
  _syncQueue?.enqueue();
}

// Quick action — sets balance to value AND balanceAsOf to today.
async function updateAccountBalance(id, balance, asOfIso = null) {
  const today = asOfIso || (new Date().toISOString().slice(0, 10));
  return updateAccount(id, { balance: Number(balance), balanceAsOf: today });
}

function _pickAccountColor() {
  const used = new Set(budgetAccounts.map(a => a.color));
  for (const c of _ACCOUNT_PALETTE) if (!used.has(c)) return c;
  return _ACCOUNT_PALETTE[budgetAccounts.length % _ACCOUNT_PALETTE.length];
}

// ── Income template CRUD (mirrors bill template structure) ─────────────────
async function createIncomeTemplate(input) {
  const tpl = {
    id:             'inc_' + (typeof uid === 'function' ? uid() : Date.now() + '_' + Math.random().toString(36).slice(2, 7)),
    name:           (input.name || '').trim() || 'Untitled income',
    amount:         Number(input.amount) || 0,
    variableAmount: !!input.variableAmount,
    frequency:      input.frequency || { unit: 'month', interval: 1, anchorMonth: null },
    dayOfMonth:     Math.max(1, Math.min(31, Number(input.dayOfMonth) || 25)),
    accountId:      input.accountId || (getPrimaryAccount()?.id ?? null),
    notes:          input.notes || '',
    archived:       false,
    createdAt:      _nowIso(),
    updatedAt:      _nowIso(),
  };
  incomeTemplates.push(tpl);
  await saveBudgetAccountsAndIncomeLocal();
  _syncQueue?.enqueue();
  return tpl;
}

async function updateIncomeTemplate(id, patch) {
  const idx = incomeTemplates.findIndex(t => t.id === id);
  if (idx === -1) return null;
  const prev = incomeTemplates[idx];
  incomeTemplates[idx] = { ...prev, ...patch, updatedAt: _nowIso() };

  // If anything that affects the projection has changed, prune unpaid
  // materialised instances for the current month onward and re-materialise
  // so the projection picks up the new schedule/amount immediately.
  const scheduleChanged =
       (patch.dayOfMonth   !== undefined && patch.dayOfMonth   !== prev.dayOfMonth)
    || (patch.amount       !== undefined && patch.amount       !== prev.amount)
    || (patch.accountId    !== undefined && patch.accountId    !== prev.accountId)
    || (patch.frequency    !== undefined && JSON.stringify(patch.frequency) !== JSON.stringify(prev.frequency));
  if (scheduleChanged) {
    const todayMonth = _yyyymm(new Date());
    await _pruneUnpaidIncomeEntriesForTemplate(id, todayMonth);
    // Re-materialise the current month so the new entries appear immediately.
    if (typeof materialiseMonth === 'function' && _budgetViewMonth) {
      await materialiseMonth(_budgetViewMonth, { force: false, persist: true });
    } else {
      await materialiseMonth(todayMonth, { force: false, persist: true });
    }
  }

  await saveBudgetAccountsAndIncomeLocal();
  _syncQueue?.enqueue();
  return incomeTemplates[idx];
}

async function archiveIncomeTemplate(id) {
  // Also prune any unpaid materialised entries — once archived, the template
  // shouldn't keep producing phantom income in the projection.
  await _pruneUnpaidIncomeEntriesForTemplate(id, /* fromMonth */ null);
  return updateIncomeTemplate(id, { archived: true });
}

async function deleteIncomeTemplateHard(id) {
  // Strip any unpaid template-instance entries from the materialised store
  // BEFORE removing the template. Paid-out entries are kept as historical
  // record (the money actually arrived).
  await _pruneUnpaidIncomeEntriesForTemplate(id, /* fromMonth */ null);
  incomeTemplates = incomeTemplates.filter(t => t.id !== id);
  incomeTemplateDeletedIds.add(id);
  await saveBudgetAccountsAndIncomeLocal();
  _syncQueue?.enqueue();
}

// Removes all unpaid template-instance income entries for a given template.
// `fromMonth` (YYYY-MM, optional): if given, only prunes entries from that
// month onward. Useful when editing a template — we don't want to wipe past
// months that have already been reconciled.
async function _pruneUnpaidIncomeEntriesForTemplate(templateId, fromMonth = null) {
  let touched = false;
  for (const yyyymm of Object.keys(incomeEntries)) {
    if (fromMonth && yyyymm < fromMonth) continue;
    const month = incomeEntries[yyyymm];
    for (const entryId of Object.keys(month)) {
      const e = month[entryId];
      if (e.templateId !== templateId) continue;
      if (e.paidAt) continue; // keep — actual money received
      delete month[entryId];
      // Re-materialisation needs to be allowed to re-create entries for
      // updated templates, so drop the month from materialisedMonths too.
      const mIdx = budgetSettings.materialisedMonths?.indexOf(yyyymm) ?? -1;
      if (mIdx >= 0) budgetSettings.materialisedMonths.splice(mIdx, 1);
      touched = true;
    }
    if (Object.keys(month).length === 0) delete incomeEntries[yyyymm];
  }
  if (touched) {
    await saveBudgetAccountsAndIncomeLocal();
    if (typeof saveBudgetLocal === 'function') await saveBudgetLocal();
    _syncQueue?.enqueue();
  }
  return touched;
}

async function createIncomeEntry(input) {
  const date = input.date || (new Date().toISOString().slice(0, 10));
  const yyyymm = _yyyymmFromString(date);
  const entry = {
    id:         'incE_' + (typeof uid === 'function' ? uid() : Date.now() + '_' + Math.random().toString(36).slice(2, 7)),
    date,
    amount:     Number(input.amount) || 0,
    source:     input.source || 'manual',          // 'manual' | 'template_instance'
    templateId: input.templateId || null,
    accountId:  input.accountId || (getPrimaryAccount()?.id ?? null),
    notes:      input.notes || '',
    paidAt:     input.paidAt || null,               // null = expected/projected
    createdAt:  _nowIso(),
    createdBy:  _kvEmailHash || null,
    updatedAt:  _nowIso(),
  };
  if (!incomeEntries[yyyymm]) incomeEntries[yyyymm] = {};
  incomeEntries[yyyymm][entry.id] = entry;
  await saveBudgetAccountsAndIncomeLocal();
  _syncQueue?.enqueue();
  return entry;
}

async function updateIncomeEntry(id, patch) {
  const located = _findIncomeEntry(id);
  if (!located) return null;
  const { yyyymm: oldYm, entry: existing } = located;
  const merged = { ...existing, ...patch, updatedAt: _nowIso() };
  const newYm = patch.date ? _yyyymmFromString(patch.date) : oldYm;

  if (newYm !== oldYm) {
    delete incomeEntries[oldYm][id];
    if (Object.keys(incomeEntries[oldYm]).length === 0) delete incomeEntries[oldYm];
    if (!incomeEntries[newYm]) incomeEntries[newYm] = {};
    incomeEntries[newYm][id] = merged;
  } else {
    incomeEntries[oldYm][id] = merged;
  }
  await saveBudgetAccountsAndIncomeLocal();
  _syncQueue?.enqueue();
  return merged;
}

async function markIncomeEntryReceived(id) {
  return updateIncomeEntry(id, { paidAt: _nowIso() });
}

async function deleteIncomeEntry(id) {
  const located = _findIncomeEntry(id);
  if (!located) return false;
  delete incomeEntries[located.yyyymm][id];
  if (Object.keys(incomeEntries[located.yyyymm]).length === 0) {
    delete incomeEntries[located.yyyymm];
  }
  incomeEntryDeletedIds.add(id);
  await saveBudgetAccountsAndIncomeLocal();
  _syncQueue?.enqueue();
  return true;
}

function _findIncomeEntry(id) {
  for (const yyyymm of Object.keys(incomeEntries)) {
    if (incomeEntries[yyyymm][id]) return { yyyymm, entry: incomeEntries[yyyymm][id] };
  }
  return null;
}

function getIncomeEntriesForMonth(yyyymm) {
  return Object.values(incomeEntries[yyyymm] || {});
}

function getIncomeEntriesForRange(startIso, endIso) {
  const out = [];
  const startMonth = _yyyymmFromString(startIso);
  const endMonth   = _yyyymmFromString(endIso);
  for (const yyyymm of Object.keys(incomeEntries)) {
    if (yyyymm < startMonth || yyyymm > endMonth) continue;
    for (const e of Object.values(incomeEntries[yyyymm])) {
      if (e.date >= startIso && e.date <= endIso) out.push(e);
    }
  }
  return out;
}

// ── Cash flow projection engine ────────────────────────────────────────────
//
// Projects an account's balance forward day-by-day for `daysAhead` days.
// Returns a structured result the chart and calendar consume directly.
//
//   projection = {
//     account,                    // the account being projected
//     startDate,                  // ISO date — projection begins from this day
//     startBalance,               // balance at startDate
//     points: [                   // one entry per day (today through today+daysAhead)
//       {
//         date:    'YYYY-MM-DD',
//         balance: 1234.56,         // running balance AFTER this day's events apply
//         events:  [{ type:'bill'|'income'|'tx', amount, label, sourceId }]
//       }, ...
//     ],
//     low:       { date, balance, daysFromStart },  // lowest balance in the window
//     hasGaps:   boolean,           // true if balanceAsOf was older than today (we did catch-up)
//     setupComplete: boolean,       // false if no primary account or no balance set
//   }
function getCashFlowLowPoint(accountId = null, daysAhead = 30) {
  const proj = projectCashFlow(accountId, daysAhead);
  if (!proj.setupComplete) return null;
  return proj.low;
}

// ── Sync merge ─────────────────────────────────────────────────────────────
let _accountsEditingId       = null;   // account being edited
let _incomeTplEditingId      = null;   // income template being edited
let _incomeEntryEditingId    = null;   // income entry being edited
let _calendarViewMonth       = null;   // 'YYYY-MM' for the cash flow detail modal
let _balanceUpdateAccountId  = null;   // account whose balance is being updated

// ── Render entry — called by renderBudget when panel === 'accounts' ────────
function renderBudgetAccounts() {
  _renderAccountsList();
  _renderIncomeTemplatesList();
  _renderIncomeEntriesList();
}

// ── Accounts list ──────────────────────────────────────────────────────────
function _renderAccountsList() {
  const host = document.getElementById('budget-accounts-list');
  if (!host) return;

  const active   = budgetAccounts.filter(a => !a.archived);
  const archived = budgetAccounts.filter(a =>  a.archived);

  if (active.length === 0) {
    host.innerHTML = `
      <div style="padding:30px 20px;text-align:center;color:var(--muted)">
        <div style="margin-bottom:10px;color:var(--accent)"><svg aria-hidden="true" style="width:42px;height:42px"><use href="#i-banknote"></use></svg></div>
        <div style="font-size:15px;font-weight:600;margin-bottom:6px;color:var(--text)">No accounts yet</div>
        <p style="font-size:13px;line-height:1.5;margin-bottom:16px;max-width:320px;margin-left:auto;margin-right:auto">
          Add the bank account, savings, or credit card you want to track. The cash flow projection on the dashboard uses your primary account.
        </p>
        <button class="btn btn-primary" onclick="openAccountEditor()"><svg class="icon" aria-hidden="true"><use href="#i-plus"></use></svg> Add your first account</button>
      </div>`;
    hide('budget-accounts-archived-section');
    return;
  }

  host.innerHTML = `<div class="bill-list">${active.map(_renderAccountRow).join('')}</div>`;

  const archivedSection = document.getElementById('budget-accounts-archived-section');
  const archivedHost    = document.getElementById('budget-accounts-archived-list');
  if (archived.length) {
    archivedSection.style.display = '';
    archivedHost.innerHTML = `<div class="bill-list">${archived.map(_renderAccountRow).join('')}</div>`;
  } else {
    archivedSection.style.display = 'none';
  }
}

function _renderAccountRow(acc) {
  const typeLabel = acc.type === 'credit_card' ? 'Credit card' : acc.type === 'savings' ? 'Savings' : 'Current';
  const asOfLabel = (() => {
    if (!acc.balanceAsOf) return '';
    const d = new Date(acc.balanceAsOf + 'T12:00:00');
    const today = new Date(); today.setHours(0,0,0,0);
    const dDate = new Date(acc.balanceAsOf + 'T00:00:00');
    const days = Math.round((today - dDate) / MS_PER_DAY);
    if (days === 0)  return 'as of today';
    if (days === 1)  return 'as of yesterday';
    if (days < 30)   return `as of ${days} days ago`;
    return 'as of ' + d.toLocaleDateString(undefined, { day:'numeric', month:'short' });
  })();
  const stale = acc.balanceAsOf && (Date.now() - new Date(acc.balanceAsOf + 'T12:00:00').getTime()) > 14 * MS_PER_DAY;

  const primaryBadge = acc.isPrimary && !acc.archived
    ? '<span class="bill-tag" style="background:rgba(232,168,56,0.15);color:var(--accent);border-color:rgba(232,168,56,0.3)">PRIMARY</span>'
    : '';

  const balanceClass = acc.balance < 0 ? 'is-overdue' : '';

  return `
    <div class="bill-row ${acc.archived ? 'is-skipped' : ''} ${balanceClass}" onclick="openAccountEditor('${acc.id}')" style="cursor:pointer">
      <div class="bill-day" style="background:${acc.color || 'var(--surface)'};color:#000;border-color:${acc.color || 'var(--border)'};font-size:9px">
        ${acc.type === 'credit_card' ? 'CC' : acc.type === 'savings' ? 'S' : (currencySymbol()||'$')}
      </div>
      <div class="bill-info">
        <div class="bill-name">${_escapeHtml(acc.name)} ${primaryBadge}</div>
        <div class="bill-meta">${typeLabel} · ${asOfLabel}${stale ? ' <span style="color:var(--warn)">⚠ update</span>' : ''}</div>
      </div>
      <div class="bill-amount">${_money(acc.balance)}</div>
      <div class="bill-actions">
        ${!acc.archived
          ? `<button class="bill-action-btn" onclick="event.stopPropagation();openUpdateBalanceModal('${acc.id}')" title="Update balance"><svg aria-hidden="true"><use href="#i-refresh-cw"></use></svg></button>
             <button class="bill-action-btn bill-action-edit" onclick="event.stopPropagation();openAccountEditor('${acc.id}')" title="Edit"><svg aria-hidden="true"><use href="#i-pencil"></use></svg></button>`
          : `<button class="bill-action-btn" onclick="event.stopPropagation();handleUnarchiveAccount('${acc.id}')" title="Unarchive"><svg aria-hidden="true"><use href="#i-refresh-cw"></use></svg></button>`
        }
      </div>
    </div>`;
}

async function handleUnarchiveAccount(id) {
  await unarchiveAccount(id);
  renderBudgetAccounts();
  if (_currentView === 'budget') renderBudget();
}

// ── Account editor modal ───────────────────────────────────────────────────
function openAccountEditor(id = null) {
  _accountsEditingId = id;
  const acc = id ? getAccountById(id) : null;
  document.getElementById('account-editor-mode-label').textContent = acc ? 'Edit Account' : 'Add Account';
  document.getElementById('account-editor-save-label').textContent = acc ? 'Save' : 'Create';
  toggle('account-archive-btn', acc && !acc.archived, 'inline-flex');
  toggle('account-delete-btn', acc &&  acc.archived, 'inline-flex');

  document.getElementById('account-name').value         = acc?.name        || '';
  document.getElementById('account-type').value         = acc?.type        || 'current';
  document.getElementById('account-balance').value      = acc?.balance     ?? '';
  document.getElementById('account-balance-asof').value = acc?.balanceAsOf || (new Date().toISOString().slice(0, 10));
  document.getElementById('account-primary').checked    = !!acc?.isPrimary;
  document.getElementById('account-color').value        = acc?.color       || '#5b8dee';
  document.getElementById('account-notes').value        = acc?.notes       || '';

  // If this is the first/only account, force primary on and disable
  const primaryCheckbox = document.getElementById('account-primary');
  const otherActive = budgetAccounts.filter(a => !a.archived && a.id !== id);
  if (otherActive.length === 0 && !acc?.archived) {
    primaryCheckbox.checked = true;
    primaryCheckbox.disabled = true;
    document.getElementById('account-primary-hint').textContent = 'First account is automatically primary.';
  } else {
    primaryCheckbox.disabled = false;
    document.getElementById('account-primary-hint').textContent = 'Cash flow projection uses the primary account.';
  }

  openModal('account-editor-modal');
  setTimeout(() => document.getElementById('account-name').focus(), 50);
}

async function saveAccountFromEditor() {
  const name = (document.getElementById('account-name').value || '').trim();
  if (!name) { toast('Account needs a name'); return; }
  const type = document.getElementById('account-type').value;
  const balance = parseFloat(document.getElementById('account-balance').value);
  if (isNaN(balance)) { toast('Enter a valid balance'); return; }
  const balanceAsOf = document.getElementById('account-balance-asof').value || (new Date().toISOString().slice(0, 10));
  const isPrimary = document.getElementById('account-primary').checked;
  const color = document.getElementById('account-color').value || '#5b8dee';
  const notes = (document.getElementById('account-notes').value || '').trim();

  const patch = { name, type, balance, balanceAsOf, isPrimary, color, notes };

  if (_accountsEditingId) {
    await updateAccount(_accountsEditingId, patch);
    toast('Account updated');
  } else {
    await createAccount(patch);
    toast('Account added');
  }

  closeModal('account-editor-modal');
  _accountsEditingId = null;
  renderBudgetAccounts();
  if (_currentView === 'budget') renderBudget();
}

async function confirmArchiveAccount() {
  if (!_accountsEditingId) return;
  const acc = getAccountById(_accountsEditingId);
  if (!acc) return;
  if (!confirm(`Archive "${acc.name}"? It won't appear in projections or as a destination for new bills/income.`)) return;
  await archiveAccount(_accountsEditingId);
  closeModal('account-editor-modal');
  _accountsEditingId = null;
  toast('Account archived');
  renderBudgetAccounts();
  if (_currentView === 'budget') renderBudget();
}

async function confirmDeleteAccount() {
  if (!_accountsEditingId) return;
  const acc = getAccountById(_accountsEditingId);
  if (!acc) return;
  if (!confirm(`Permanently delete "${acc.name}"? Bills/income that pointed to this account will need to be reassigned. This can't be undone.`)) return;
  await deleteAccountHard(_accountsEditingId);
  closeModal('account-editor-modal');
  _accountsEditingId = null;
  toast('Account deleted');
  renderBudgetAccounts();
  if (_currentView === 'budget') renderBudget();
}

// ── Update balance quick action ────────────────────────────────────────────
function openUpdateBalanceModal(accountId) {
  const acc = getAccountById(accountId);
  if (!acc) return;
  _balanceUpdateAccountId = accountId;
  document.getElementById('update-balance-name').textContent = acc.name;
  const input = document.getElementById('update-balance-amount');
  input.value = acc.balance;
  document.getElementById('update-balance-current').textContent =
    `Current: ${_money(acc.balance)} (as of ${acc.balanceAsOf || 'never'})`;
  openModal('update-balance-modal');
  setTimeout(() => input.select(), 50);
}

async function confirmUpdateBalance() {
  if (!_balanceUpdateAccountId) return;
  const amt = parseFloat(document.getElementById('update-balance-amount').value);
  if (isNaN(amt)) { toast('Enter a valid amount'); return; }
  await updateAccountBalance(_balanceUpdateAccountId, amt);
  closeModal('update-balance-modal');
  _balanceUpdateAccountId = null;
  toast('Balance updated');
  renderBudgetAccounts();
  if (_currentView === 'budget') renderBudget();
}

// ── Income templates ───────────────────────────────────────────────────────
function _renderIncomeTemplatesList() {
  const section = document.getElementById('budget-income-templates-section');
  const host    = document.getElementById('budget-income-templates-list');
  if (!section || !host) return;

  const active = incomeTemplates.filter(t => !t.archived);
  if (active.length === 0) {
    host.innerHTML = `
      <div style="padding:18px;text-align:center;color:var(--muted);font-size:12px">
        No income templates. Add salaries, benefits, or any recurring income.
      </div>`;
  } else {
    host.innerHTML = `<div class="bill-list">${active.map(_renderIncomeTemplateRow).join('')}</div>`;
  }
}

function _renderIncomeTemplateRow(tpl) {
  const acc = getAccountById(tpl.accountId);
  const accLabel = acc ? acc.name : '(no account)';
  const freq = _frequencyLabel(tpl);
  return `
    <div class="bill-row" onclick="openIncomeTemplateEditor('${tpl.id}')" style="cursor:pointer">
      <div class="bill-day" style="background:rgba(76,187,138,0.15);color:var(--ok);border-color:rgba(76,187,138,0.4)">${tpl.dayOfMonth}</div>
      <div class="bill-info">
        <div class="bill-name">${_escapeHtml(tpl.name)}</div>
        <div class="bill-meta">${freq} · ${_escapeHtml(accLabel)}</div>
      </div>
      <div class="bill-amount" style="color:var(--ok)">+${_money(tpl.amount)}</div>
      <div class="bill-actions">
        <button class="bill-action-btn bill-action-edit" onclick="event.stopPropagation();openIncomeTemplateEditor('${tpl.id}')" title="Edit"><svg aria-hidden="true"><use href="#i-pencil"></use></svg></button>
      </div>
    </div>`;
}

function openIncomeTemplateEditor(id = null) {
  _incomeTplEditingId = id;
  const tpl = id ? getIncomeTemplateById(id) : null;
  document.getElementById('income-tpl-mode-label').textContent = tpl ? 'Edit Income' : 'Add Income';
  document.getElementById('income-tpl-save-label').textContent = tpl ? 'Save' : 'Add';
  toggle('income-tpl-archive-btn', tpl, 'inline-flex');

  document.getElementById('income-tpl-name').value         = tpl?.name      || '';
  document.getElementById('income-tpl-amount').value       = tpl?.amount    ?? '';
  document.getElementById('income-tpl-day').value          = tpl?.dayOfMonth ?? 25;
  document.getElementById('income-tpl-notes').value        = tpl?.notes     || '';

  // Frequency presets — same as bill editor
  const freq = tpl?.frequency || { unit: 'month', interval: 1, anchorMonth: null };
  let preset = 'monthly';
  if (freq.unit === 'year') preset = 'annual';
  else if (freq.interval === 3)  preset = 'quarterly';
  else if (freq.interval === 6)  preset = 'six_monthly';
  else if (freq.interval === 1)  preset = 'monthly';
  else                           preset = 'custom';
  document.getElementById('income-tpl-frequency').value = preset;
  document.getElementById('income-tpl-anchor-month').value = String(freq.anchorMonth ?? 0);
  document.getElementById('income-tpl-custom-interval').value = (preset === 'custom' ? freq.interval : 2);

  // Account dropdown
  const accSel = document.getElementById('income-tpl-account');
  const accs = getActiveAccounts();
  const primaryId = getPrimaryAccount()?.id;
  accSel.innerHTML = accs.map(a =>
    `<option value="${a.id}" ${a.id === (tpl?.accountId || primaryId) ? 'selected' : ''}>${_escapeHtml(a.name)}${a.isPrimary ? ' (primary)' : ''}</option>`
  ).join('') || '<option value="">— No accounts —</option>';

  incomeTplFreqChanged();
  openModal('income-tpl-modal');
  setTimeout(() => document.getElementById('income-tpl-name').focus(), 50);
}

function incomeTplFreqChanged() {
  const preset = document.getElementById('income-tpl-frequency').value;
  toggle('income-tpl-anchor-row', preset !== 'monthly', 'block');
  toggle('income-tpl-custom-row', preset === 'custom', 'block');
}

async function saveIncomeTemplateFromEditor() {
  const name = (document.getElementById('income-tpl-name').value || '').trim();
  if (!name) { toast('Income needs a name'); return; }
  const amount = parseFloat(document.getElementById('income-tpl-amount').value);
  if (isNaN(amount) || amount <= 0) { toast('Enter a valid amount'); return; }
  const dayOfMonth = parseInt(document.getElementById('income-tpl-day').value, 10);
  if (isNaN(dayOfMonth) || dayOfMonth < 1 || dayOfMonth > 31) { toast('Day must be 1-31'); return; }
  const notes = (document.getElementById('income-tpl-notes').value || '').trim();
  const accountId = document.getElementById('income-tpl-account').value || null;

  const preset = document.getElementById('income-tpl-frequency').value;
  const anchorMonth = parseInt(document.getElementById('income-tpl-anchor-month').value, 10);
  const customInt = parseInt(document.getElementById('income-tpl-custom-interval').value, 10);
  let frequency;
  if (preset === 'monthly')           frequency = { unit: 'month', interval: 1, anchorMonth: null };
  else if (preset === 'quarterly')    frequency = { unit: 'month', interval: 3, anchorMonth };
  else if (preset === 'six_monthly')  frequency = { unit: 'month', interval: 6, anchorMonth };
  else if (preset === 'annual')       frequency = { unit: 'year',  interval: 1, anchorMonth };
  else                                frequency = { unit: 'month', interval: Math.max(1, customInt || 2), anchorMonth };

  const patch = { name, amount, dayOfMonth, notes, accountId, frequency };

  if (_incomeTplEditingId) {
    await updateIncomeTemplate(_incomeTplEditingId, patch);
    toast('Income updated');
  } else {
    await createIncomeTemplate(patch);
    toast('Income added');
  }
  closeModal('income-tpl-modal');
  _incomeTplEditingId = null;
  renderBudgetAccounts();
  if (_currentView === 'budget') renderBudget();
}

async function confirmArchiveIncomeTemplate() {
  if (!_incomeTplEditingId) return;
  const tpl = getIncomeTemplateById(_incomeTplEditingId);
  if (!tpl) return;
  if (!confirm(`Archive "${tpl.name}"? Past instances stay; the template won't generate new ones.`)) return;
  await archiveIncomeTemplate(_incomeTplEditingId);
  closeModal('income-tpl-modal');
  _incomeTplEditingId = null;
  toast('Income archived');
  renderBudgetAccounts();
  if (_currentView === 'budget') renderBudget();
}

// ── One-off income entries ─────────────────────────────────────────────────
function _renderIncomeEntriesList() {
  const section = document.getElementById('budget-income-entries-section');
  const host    = document.getElementById('budget-income-entries-list');
  if (!section || !host) return;

  // Show last 30 days of one-off (non-template) entries
  const today = new Date();
  const past = new Date(today.getTime() - 30 * MS_PER_DAY);
  const startIso = past.toISOString().slice(0, 10);
  const endIso   = today.toISOString().slice(0, 10);

  const entries = getIncomeEntriesForRange(startIso, endIso)
    .filter(e => !e.templateId)
    .sort((a, b) => b.date.localeCompare(a.date));

  if (entries.length === 0) {
    host.innerHTML = `
      <div style="padding:14px;text-align:center;color:var(--muted);font-size:12px">
        No one-off income in the last 30 days.
        <button class="btn btn-ghost btn-sm" onclick="openIncomeEntryEditor()" style="margin-top:8px;font-size:11px"><svg class="icon" aria-hidden="true"><use href="#i-plus"></use></svg> Add bonus / refund / gift</button>
      </div>`;
  } else {
    host.innerHTML = `<div class="bill-list">${entries.map(_renderIncomeEntryRow).join('')}</div>`;
  }
}

function _renderIncomeEntryRow(entry) {
  const acc = getAccountById(entry.accountId);
  const accLabel = acc ? acc.name : '(no account)';
  const dayLabel = (() => {
    const d = new Date(entry.date + 'T12:00:00');
    return d.toLocaleDateString(undefined, { day: 'numeric', month: 'short' });
  })();
  const isFuture = entry.date > new Date().toISOString().slice(0, 10);
  return `
    <div class="bill-row" onclick="openIncomeEntryEditor('${entry.id}')" style="cursor:pointer">
      <div class="bill-day" style="background:rgba(76,187,138,0.15);color:var(--ok);border-color:rgba(76,187,138,0.4);font-size:10px">${entry.date.slice(8, 10)}</div>
      <div class="bill-info">
        <div class="bill-name">${_escapeHtml(entry.notes || 'Income')}</div>
        <div class="bill-meta">${dayLabel} · ${_escapeHtml(accLabel)}${isFuture ? ' · upcoming' : ''}</div>
      </div>
      <div class="bill-amount" style="color:var(--ok)">+${_money(entry.amount)}</div>
      <div class="bill-actions">
        <button class="bill-action-btn bill-action-edit" onclick="event.stopPropagation();openIncomeEntryEditor('${entry.id}')" title="Edit"><svg aria-hidden="true"><use href="#i-pencil"></use></svg></button>
      </div>
    </div>`;
}

function openIncomeEntryEditor(id = null) {
  _incomeEntryEditingId = id;
  const entry = id ? getIncomeEntriesForRange('1970-01-01', '2099-12-31').find(e => e.id === id) : null;
  document.getElementById('income-entry-mode-label').textContent = entry ? 'Edit Income Entry' : 'Add Income Entry';
  document.getElementById('income-entry-save-label').textContent = entry ? 'Save' : 'Add';
  toggle('income-entry-delete-btn', entry, 'inline-flex');

  document.getElementById('income-entry-notes').value  = entry?.notes  || '';
  document.getElementById('income-entry-amount').value = entry?.amount ?? '';
  document.getElementById('income-entry-date').value   = entry?.date   || (new Date().toISOString().slice(0, 10));

  // Account dropdown
  const accSel = document.getElementById('income-entry-account');
  const accs = getActiveAccounts();
  const primaryId = getPrimaryAccount()?.id;
  accSel.innerHTML = accs.map(a =>
    `<option value="${a.id}" ${a.id === (entry?.accountId || primaryId) ? 'selected' : ''}>${_escapeHtml(a.name)}${a.isPrimary ? ' (primary)' : ''}</option>`
  ).join('') || '<option value="">— No accounts —</option>';

  openModal('income-entry-modal');
  setTimeout(() => document.getElementById('income-entry-notes').focus(), 50);
}

async function saveIncomeEntryFromEditor() {
  const notes  = (document.getElementById('income-entry-notes').value || '').trim();
  const amount = parseFloat(document.getElementById('income-entry-amount').value);
  if (isNaN(amount) || amount <= 0) { toast('Enter a valid amount'); return; }
  const date = document.getElementById('income-entry-date').value;
  if (!date) { toast('Pick a date'); return; }
  const accountId = document.getElementById('income-entry-account').value || null;

  if (_incomeEntryEditingId) {
    await updateIncomeEntry(_incomeEntryEditingId, { notes, amount, date, accountId });
    toast('Income entry updated');
  } else {
    await createIncomeEntry({ notes, amount, date, accountId, source: 'manual' });
    toast('Income entry added');
  }
  closeModal('income-entry-modal');
  _incomeEntryEditingId = null;
  renderBudgetAccounts();
  if (_currentView === 'budget') renderBudget();
}

async function confirmDeleteIncomeEntry() {
  if (!_incomeEntryEditingId) return;
  if (!confirm('Delete this income entry?')) return;
  await deleteIncomeEntry(_incomeEntryEditingId);
  closeModal('income-entry-modal');
  _incomeEntryEditingId = null;
  toast('Income entry deleted');
  renderBudgetAccounts();
  if (_currentView === 'budget') renderBudget();
}

// ── Cash flow chart on the dashboard ───────────────────────────────────────
function renderCashFlowChart() {
  const host    = document.getElementById('budget-cashflow-card');
  if (!host) return;

  const proj = projectCashFlow(null, 30);

  if (!proj.setupComplete) {
    host.innerHTML = `
      <div style="background:var(--surface);border:1px dashed var(--border);border-radius:10px;padding:16px;text-align:center">
        <div style="font-size:10px;color:var(--muted);font-family:var(--mono);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:6px">Cash flow · 30 days</div>
        <div style="font-size:13px;color:var(--muted);margin-bottom:10px">Set up an account to see your projected balance</div>
        <button class="btn btn-primary btn-sm" onclick="navTo && navTo('budget'); budgetSwitchPanel('accounts')"><svg class="icon" aria-hidden="true"><use href="#i-plus"></use></svg> Add account</button>
      </div>`;
    return;
  }

  const lowAlert = (proj.low && proj.low.balance < 0)
    ? `<span style="color:var(--danger);font-weight:700">${_money(proj.low.balance)}</span> low`
    : (proj.low ? `<span style="color:var(--text);font-weight:600">${_money(proj.low.balance)}</span> low` : '');

  host.innerHTML = `
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:14px 16px">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
        <div style="font-size:13px;font-weight:600">Cash flow · 30 days</div>
        <div style="display:flex;gap:6px;align-items:center">
          <span style="font-size:11px;color:var(--muted);font-family:var(--mono)">${_escapeHtml(proj.account.name)}</span>
          <button class="btn btn-ghost btn-sm" onclick="openCashFlowCalendar()" title="Calendar view" style="padding:4px 7px"><svg class="icon" aria-hidden="true"><use href="#i-calendar"></use></svg></button>
        </div>
      </div>
      <div id="cashflow-svg-host">${_buildCashFlowSvg(proj)}</div>
      <div style="display:flex;gap:14px;margin-top:8px;font-size:10px;color:var(--muted);font-family:var(--mono)">
        <span style="display:inline-flex;align-items:center;gap:5px"><span style="width:7px;height:7px;border-radius:50%;background:var(--danger)"></span>bill</span>
        <span style="display:inline-flex;align-items:center;gap:5px"><span style="width:7px;height:7px;border-radius:50%;background:var(--ok)"></span>payday</span>
        <span style="margin-left:auto">${lowAlert}</span>
      </div>
      ${proj.hasGaps ? `<div style="margin-top:8px;padding:6px 10px;background:rgba(232,168,56,0.08);border:1px solid rgba(232,168,56,0.2);border-radius:6px;font-size:11px;color:var(--warn)">
        <svg aria-hidden="true" style="width:11px;height:11px;vertical-align:-1px"><use href="#i-alert-triangle"></use></svg>
        Balance was last updated ${(() => {
          const d = new Date(proj.account.balanceAsOf + 'T12:00:00');
          const days = Math.round((Date.now() - d.getTime()) / MS_PER_DAY);
          return days + ' day' + (days === 1 ? '' : 's') + ' ago';
        })()} — projection assumes bills paid + income received in between.
        <button class="btn btn-ghost btn-sm" style="margin-left:6px;font-size:11px;padding:2px 8px" onclick="openUpdateBalanceModal('${proj.account.id}')">Update</button>
      </div>` : ''}
    </div>`;
}

function _buildCashFlowSvg(proj) {
  // SVG dimensions
  const W = 480, H = 90;
  const padL = 4, padR = 4, padT = 12, padB = 14;
  const innerW = W - padL - padR;
  const innerH = H - padT - padB;

  const points = proj.points;
  if (points.length === 0) return '<svg viewBox="0 0 480 90" width="100%" height="90" aria-hidden="true"></svg>';

  // Y range — include startBalance and zero line for context
  const balances = points.map(p => p.balance);
  const minB = Math.min(...balances, proj.startBalance, 0);
  const maxB = Math.max(...balances, proj.startBalance);
  const range = (maxB - minB) || 1;

  const xFor = (i) => padL + (i / Math.max(points.length - 1, 1)) * innerW;
  const yFor = (b) => padT + (1 - (b - minB) / range) * innerH;

  // Zero line if relevant
  let zeroLine = '';
  if (minB < 0 && maxB > 0) {
    const y0 = yFor(0);
    zeroLine = `<line x1="${padL}" y1="${y0}" x2="${W - padR}" y2="${y0}" stroke="var(--border)" stroke-width="0.5" stroke-dasharray="2,3"/>`;
  }

  // Build polyline points: each day stays flat then drops/rises (steppy line that
  // looks more like a bank balance). Simpler: just connect each point.
  // For a more honest cash-flow look, we use a stepped line: balance is flat
  // through a day, then jumps at the day boundary.
  const linePoints = points.map((p, i) => `${xFor(i).toFixed(1)},${yFor(p.balance).toFixed(1)}`).join(' ');

  // Markers for events — bill = red dot, income = green dot
  const markers = points.map((p, i) => {
    if (!p.events || p.events.length === 0) return '';
    const cx = xFor(i).toFixed(1);
    const cy = yFor(p.balance).toFixed(1);
    const hasBill   = p.events.some(e => e.type === 'bill');
    const hasIncome = p.events.some(e => e.type === 'income');
    // If both, show two stacked
    if (hasBill && hasIncome) {
      return `<circle cx="${cx}" cy="${cy}" r="3" fill="var(--ok)"/><circle cx="${cx}" cy="${(yFor(p.balance) - 6).toFixed(1)}" r="3" fill="var(--danger)"/>`;
    }
    const fill = hasBill ? 'var(--danger)' : 'var(--ok)';
    return `<circle cx="${cx}" cy="${cy}" r="3" fill="${fill}"/>`;
  }).join('');

  // Low-point callout
  let callout = '';
  if (proj.low) {
    const idx = proj.low.daysFromStart;
    const cx = xFor(idx);
    const cy = yFor(proj.low.balance);
    // Vertical guide line
    callout += `<line x1="${cx}" y1="${padT}" x2="${cx}" y2="${cy + 2}" stroke="rgba(232,80,80,0.35)" stroke-width="0.8" stroke-dasharray="3,3"/>`;
    // Highlighted dot
    callout += `<circle cx="${cx}" cy="${cy}" r="4" fill="${proj.low.balance < 0 ? 'var(--danger)' : 'var(--accent)'}" stroke="${proj.low.balance < 0 ? 'var(--danger)' : 'var(--accent)'}" stroke-width="3" stroke-opacity="0.25"/>`;
    // Label
    const lowDate = new Date(proj.low.date + 'T12:00:00').toLocaleDateString(undefined, { day:'numeric', month:'short' });
    // Position label so it doesn't run off the edge
    const labelX = Math.min(Math.max(cx + 6, padL), W - padR - 90);
    callout += `<text x="${labelX}" y="${H - 2}" font-size="9" font-family="var(--mono)" fill="var(--muted)">low: ${_money(proj.low.balance)} on ${lowDate}</text>`;
  }

  return `
    <svg viewBox="0 0 ${W} ${H}" width="100%" height="${H}" aria-hidden="true" style="display:block">
      ${zeroLine}
      <polyline points="${linePoints}" fill="none" stroke="var(--accent2)" stroke-width="1.5"/>
      ${markers}
      ${callout}
    </svg>`;
}

// ── Calendar detail modal (Option D) ───────────────────────────────────────
function openCashFlowCalendar() {
  if (!_calendarViewMonth) _calendarViewMonth = _yyyymm(new Date());
  _renderCashFlowCalendar();
  openModal('cashflow-calendar-modal');
}

function calendarPrevMonth() {
  const { year, month } = _parseYyyymm(_calendarViewMonth);
  const d = new Date(year, month - 1, 1);
  _calendarViewMonth = _yyyymm(d);
  _renderCashFlowCalendar();
}
function calendarNextMonth() {
  const { year, month } = _parseYyyymm(_calendarViewMonth);
  const d = new Date(year, month + 1, 1);
  _calendarViewMonth = _yyyymm(d);
  _renderCashFlowCalendar();
}

function _renderCashFlowCalendar() {
  const titleEl = document.getElementById('cashflow-calendar-title');
  const gridHost = document.getElementById('cashflow-calendar-grid');
  const summaryHost = document.getElementById('cashflow-calendar-summary');
  if (!titleEl || !gridHost) return;

  const { year, month } = _parseYyyymm(_calendarViewMonth);
  titleEl.textContent = new Date(year, month, 1).toLocaleDateString(undefined, { month: 'long', year: 'numeric' });

  const primary = getPrimaryAccount();
  if (!primary) {
    gridHost.innerHTML = '<div style="padding:30px;text-align:center;color:var(--muted)">No primary account set</div>';
    if (summaryHost) summaryHost.innerHTML = '';
    return;
  }

  // Build day-by-day events for the whole month
  const daysIn = _daysInMonth(year, month);
  const firstDow = new Date(year, month, 1).getDay(); // 0 = Sun
  const weekStart = budgetSettings.weekStart || 'mon';
  // Convert firstDow to "days from week start" (0..6)
  const offsetCells = weekStart === 'mon' ? ((firstDow + 6) % 7) : firstDow;

  const headers = weekStart === 'mon' ? ['M','T','W','T','F','S','S'] : ['S','M','T','W','T','F','S'];

  const cells = [];
  for (let i = 0; i < offsetCells; i++) {
    cells.push({ blank: true });
  }
  for (let d = 1; d <= daysIn; d++) {
    const iso = `${_calendarViewMonth}-${String(d).padStart(2, '0')}`;
    const events = _eventsOnDay(primary, iso);
    cells.push({ day: d, iso, events });
  }
  // Pad to multiple of 7
  while (cells.length % 7 !== 0) cells.push({ blank: true });

  const todayIso = new Date().toISOString().slice(0, 10);

  gridHost.innerHTML = `
    <div class="cashflow-cal-grid">
      ${headers.map(h => `<div class="cashflow-cal-head">${h}</div>`).join('')}
    </div>
    <div class="cashflow-cal-grid" style="margin-top:3px">
      ${cells.map(c => {
        if (c.blank) return '<div class="cashflow-cal-cell muted"></div>';
        const hasBill   = c.events.some(e => e.type === 'bill');
        const hasIncome = c.events.some(e => e.type === 'income');
        const isToday   = c.iso === todayIso;
        let cls = 'cashflow-cal-cell';
        if (hasBill && !hasIncome)  cls += ' is-bill';
        else if (hasIncome && !hasBill) cls += ' is-pay';
        else if (hasBill && hasIncome) cls += ' is-mixed';
        if (isToday) cls += ' is-today';
        const dayDelta = c.events.reduce((s, e) => s + e.amount, 0);
        const deltaText = dayDelta !== 0
          ? `<div class="cashflow-cal-amt" style="color:${dayDelta < 0 ? 'var(--danger)' : 'var(--ok)'}">${dayDelta < 0 ? '−' : '+'}${_money(Math.abs(dayDelta))}</div>`
          : '';
        return `<div class="${cls}" onclick="_showCalendarDayDetail('${c.iso}')">
          <div class="cashflow-cal-day">${c.day}</div>
          ${deltaText}
        </div>`;
      }).join('')}
    </div>`;

  // Summary: monthly totals
  const allEvents = cells.filter(c => !c.blank).flatMap(c => c.events);
  const totalBills  = allEvents.filter(e => e.type === 'bill').reduce((s, e) => s + e.amount, 0); // negative
  const totalIncome = allEvents.filter(e => e.type === 'income').reduce((s, e) => s + e.amount, 0); // positive
  const net = totalBills + totalIncome;
  if (summaryHost) {
    summaryHost.innerHTML = `
      <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-top:14px">
        <div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px;text-align:center">
          <div style="font-size:10px;color:var(--muted);font-family:var(--mono);text-transform:uppercase">Bills</div>
          <div style="font-size:14px;font-weight:700;color:var(--danger);font-family:var(--mono);margin-top:2px">${_money(totalBills)}</div>
        </div>
        <div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px;text-align:center">
          <div style="font-size:10px;color:var(--muted);font-family:var(--mono);text-transform:uppercase">Income</div>
          <div style="font-size:14px;font-weight:700;color:var(--ok);font-family:var(--mono);margin-top:2px">+${_money(totalIncome)}</div>
        </div>
        <div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px;text-align:center">
          <div style="font-size:10px;color:var(--muted);font-family:var(--mono);text-transform:uppercase">Net</div>
          <div style="font-size:14px;font-weight:700;color:${net < 0 ? 'var(--danger)' : 'var(--ok)'};font-family:var(--mono);margin-top:2px">${net < 0 ? '−' : '+'}${_money(Math.abs(net))}</div>
        </div>
      </div>`;
  }
}

function _showCalendarDayDetail(iso) {
  const primary = getPrimaryAccount();
  if (!primary) return;
  const events = _eventsOnDay(primary, iso);
  if (events.length === 0) return;
  const lines = events.map(e => {
    const sign = e.amount < 0 ? '−' : '+';
    return `${sign}${_money(Math.abs(e.amount))}  ${e.label}`;
  });
  const date = new Date(iso + 'T12:00:00').toLocaleDateString(undefined, { weekday: 'long', day: 'numeric', month: 'long' });
  alert(`${date}\n\n${lines.join('\n')}`);
}

// ── Bill editor extension: account dropdown ────────────────────────────────
// Wraps the existing openBillEditor to populate the new account dropdown.
const _origOpenBillEditor = (typeof openBillEditor === 'function') ? openBillEditor : null;
if (_origOpenBillEditor) {
  openBillEditor = function(billId = null) {
    _origOpenBillEditor.call(this, billId);
    // After the original editor opens, populate the account dropdown
    setTimeout(() => {
      const sel = document.getElementById('bill-account');
      if (!sel) return; // dropdown not in DOM (legacy)
      const tpl = billId ? bills.find(b => b.id === billId) : null;
      const accs = getActiveAccounts();
      const primaryId = getPrimaryAccount()?.id;
      const selectedId = tpl?.accountId || primaryId;
      sel.innerHTML = (accs.length ? '' : '<option value="">— No accounts yet —</option>') + accs.map(a =>
        `<option value="${a.id}" ${a.id === selectedId ? 'selected' : ''}>${_escapeHtml(a.name)}${a.isPrimary ? ' (primary)' : ''}</option>`
      ).join('');
    }, 0);
  };
}
// Wrap saveBillFromEditor too so it picks up the accountId from the dropdown
const _origSaveBillFromEditor = (typeof saveBillFromEditor === 'function') ? saveBillFromEditor : null;
if (_origSaveBillFromEditor) {
  saveBillFromEditor = async function() {
    // Read the accountId BEFORE calling the original (which closes the modal & resets state)
    const accSel = document.getElementById('bill-account');
    const accountId = accSel ? (accSel.value || null) : null;
    const editingId = _budgetEditingBillId;
    const billCountBefore = bills.length;
    await _origSaveBillFromEditor.call(this);
    // After save, patch the bill with accountId. We need to handle three cases:
    //   1. Edit succeeded → editingId still points at the bill, patch it
    //   2. New bill created → bills.length grew, patch the new last bill
    //   3. Validation failed in original → no DB change, do nothing
    if (!accountId) return; // user didn't pick an account, nothing to patch
    if (editingId) {
      // Edit path — patch the existing bill (validation failure here means
      // the original save would have toasted; our patch is harmless either way
      // since the bill still has the same id and accountId is a valid field)
      await updateBill(editingId, { accountId });
    } else if (bills.length > billCountBefore) {
      // New bill was successfully created — patch the new one
      const last = bills[bills.length - 1];
      if (last) await updateBill(last.id, { accountId });
    }
    // else: validation failed creating a new bill; nothing was added; do nothing
  };
}

// ── Hero card extension: projected balance + low-point ─────────────────────
function _augmentHeroWithProjection() {
  const heroPanel = document.getElementById('budget-hero');
  if (!heroPanel) return;
  const proj = projectCashFlow(null, 30);
  if (!proj.setupComplete) return; // leave Phase 1 hero as-is

  // Find or create the projected-balance row
  let projRow = document.getElementById('budget-hero-projection');
  if (!projRow) {
    projRow = document.createElement('div');
    projRow.id = 'budget-hero-projection';
    projRow.style.cssText = 'display:flex;gap:20px;margin-top:10px;padding-top:10px;border-top:1px solid var(--border)';
    heroPanel.appendChild(projRow);
  }

  const startBal = proj.startBalance;
  const lowBal   = proj.low?.balance;
  const lowDate  = proj.low ? new Date(proj.low.date + 'T12:00:00').toLocaleDateString(undefined, { day:'numeric', month:'short' }) : '';
  const lowColor = (lowBal != null && lowBal < 0) ? 'var(--danger)' : 'var(--text)';

  projRow.innerHTML = `
    <div style="flex:1;min-width:0">
      <div style="font-size:10px;color:var(--muted);font-family:var(--mono);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:3px">Balance now</div>
      <div style="font-size:14px;font-weight:600;color:var(--text)">${_money(startBal)}</div>
    </div>
    <div style="flex:1;min-width:0">
      <div style="font-size:10px;color:var(--muted);font-family:var(--mono);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:3px">Low (30d)</div>
      <div style="font-size:14px;font-weight:600;color:${lowColor}">${_money(lowBal ?? startBal)}</div>
      ${lowDate ? `<div style="font-size:10px;color:var(--muted);font-family:var(--mono)">${lowDate}</div>` : ''}
    </div>`;
}


// ═══════════════════════════════════════════════════════════════════════════
//  BUDGET — Phase 4a Foundations
//  - Generalised frequency engine (day/week/month/year)
//  - Variable income (actual amounts, last-3 averaging)
//  - Income materialisation parallel to bill instances
//
//  Insertion point: in app.js, IMMEDIATELY AFTER the Phase 3b BUDGET ACCOUNTS
//  UI block (just before the GROCERY LIST section).
// ═══════════════════════════════════════════════════════════════════════════

// ── Generalised frequency engine ───────────────────────────────────────────
//
// Frequency shape (extended in Phase 4):
//   { unit: 'day' | 'week' | 'month' | 'year', interval: N, anchorDate?: 'YYYY-MM-DD', anchorMonth?: 0..11 }
//
// - Monthly templates use anchorMonth (0=Jan) AND dayOfMonth on the template.
//   E.g. { unit: 'month', interval: 1 } + dayOfMonth=5 → 5th of every month.
//   E.g. { unit: 'month', interval: 3, anchorMonth: 2 } + dayOfMonth=10 → 10 March, 10 June, 10 Sep, 10 Dec.
//
// - Weekly/daily templates use anchorDate (the first occurrence's exact date).
//   The dayOfMonth field is ignored. interval is in weeks (or days).
//   E.g. { unit: 'week', interval: 4, anchorDate: '2026-05-05' } → every 4 weeks from 5 May.
//
// - Yearly templates use anchorMonth + dayOfMonth.
//
// Returns the list of dates (YYYY-MM-DD) where the template lands within the
// given month. May be 0, 1, or many entries (a 4-weekly template can produce
// 1 or 2 dates per calendar month).

shouldBeDueInMonth = function(template, year, monthZeroIdx) {
  return getInstanceDatesInMonth(template, year, monthZeroIdx).length > 0;
};

// Helper: format a Date object as YYYY-MM-DD
_frequencyLabel = function(tpl) {
  const f = tpl.frequency || { unit: 'month', interval: 1 };
  const interval = Math.max(1, f.interval || 1);
  if (f.unit === 'day') {
    if (interval === 1) return 'Daily';
    if (interval === 7) return 'Weekly';
    if (interval === 14) return 'Fortnightly';
    return `Every ${interval} days`;
  }
  if (f.unit === 'week') {
    if (interval === 1) return 'Weekly';
    if (interval === 2) return 'Fortnightly';
    if (interval === 4) return 'Every 4 weeks';
    return `Every ${interval} weeks`;
  }
  if (f.unit === 'year') return 'Annual';
  // 'month'
  if (interval === 1)  return 'Monthly';
  if (interval === 3)  return 'Every 3 months';
  if (interval === 6)  return 'Every 6 months';
  if (interval === 12) return 'Annual';
  return `Every ${interval} months`;
};

// ── Bill instance migration ────────────────────────────────────────────────
// Phase 1-3 stored bill instances keyed by `billId` (one per month per bill).
// Weekly/4-weekly templates can produce multiple instances per month, so we
// migrate to keys of form `${billId}__${dueDate}`. Old keys are detected by
// the absence of "__" and rewritten on first read.
//
// Phase 5b: also migrates legacy `__SAV__` saving keys to the uniform
// `billId__dueDate` format. The instance keeps `kind: 'saving'` on the
// value side, which is what the rest of the code now uses to distinguish
// saving from payment instances. Also purges any phantom instances that
// lack a dueDate or expectedAmount — these were created by a pre-fix bug
// in _setInstance that wrote partial records when the key didn't match.
materialiseMonth = async function(yyyymm, { force = false, persist = true } = {}) {
  // Migrate any old-format instances first (idempotent)
  const migrated = _migrateBillInstancesIfNeeded(yyyymm);

  const already = budgetSettings.materialisedMonths.includes(yyyymm);
  // We DON'T early-return when already-materialised. The per-template loops
  // below are already idempotent (skip keys that already exist), so running
  // them on an already-materialised month is the right way to pick up newly-
  // created bill or income templates that need instances. The previous early
  // return caused brand-new bills to silently fail to appear after save.
  // `force=true` still wipes and rebuilds (used by "Regenerate this month").

  const { year, month } = _parseYyyymm(yyyymm);
  const monthInstances  = (force ? {} : (billInstances[yyyymm] || {}));
  const beforeBillKeys  = new Set(Object.keys(monthInstances));

  for (const tpl of bills) {
    if (tpl.archived) continue;
    const dates = getInstanceDatesInMonth(tpl, year, month);
    for (const dueDate of dates) {
      const key = `${tpl.id}__${dueDate}`;
      if (!force && monthInstances[key]) continue;
      monthInstances[key] = {
        billId:         tpl.id,
        dueDate,
        expectedAmount: tpl.amount,
        actualAmount:   null,
        paidAt:         null,
        paidBy:         null,
        skipped:        false,
        source:         'manual',
        kind:           'payment',  // explicit for split-bill clarity
        updatedAt:      _nowIso(),
      };
    }

    // Phase 5: split-strategy bills also get a "saving" instance for each
    // saving month between their payment dates. Saving instances are paper-
    // only — they don't move money out of the account. Marking one paid
    // adds its per-period amount to the bill's carry-over.
    if (tpl.paymentStrategy === 'split' && tpl.splitInto && tpl.splitInto.count) {
      // Only create a saving instance if (a) this month isn't a payment
      // month for the bill, and (b) the month falls within an active
      // saving cycle (between two payments, or after the bill's first
      // saving month if it's brand new).
      const isPaymentMonth = dates.length > 0;
      if (!isPaymentMonth && _isSplitBillSavingMonth(tpl, year, month)) {
        const dom    = _clampDayOfMonth(tpl.dayOfMonth || 1, year, month);
        const dueDate = _isoDate(year, month, dom);
        // Saving instances share the same key format as payment instances
        // (`billId__dueDate`). They live in different calendar months than
        // their bill's payment instance so there's no collision. The `kind`
        // field on the value distinguishes saving from payment.
        const savingKey = `${tpl.id}__${dueDate}`;
        const perPeriod = Math.round((tpl.amount / tpl.splitInto.count) * 100) / 100;
        if (force || !monthInstances[savingKey]) {
          monthInstances[savingKey] = {
            billId:         tpl.id,
            dueDate,
            expectedAmount: perPeriod,
            actualAmount:   null,
            paidAt:         null,
            paidBy:         null,
            skipped:        false,
            source:         'split-saving',
            kind:           'saving',
            updatedAt:      _nowIso(),
          };
        }
      }
    }
  }
  const billsChanged = force || (Object.keys(monthInstances).length !== beforeBillKeys.size);

  // Phase 4: materialise income too — same idempotent pattern
  if (!incomeEntries[yyyymm]) incomeEntries[yyyymm] = {};
  const monthIncomeEntries = incomeEntries[yyyymm];
  const beforeIncomeKeys = new Set(Object.keys(monthIncomeEntries));
  for (const tpl of (incomeTemplates || [])) {
    if (tpl.archived) continue;
    const dates = getInstanceDatesInMonth(tpl, year, month);
    for (const dueDate of dates) {
      // Use a deterministic id so re-materialisation doesn't duplicate
      const id = `incTpl_${tpl.id}__${dueDate}`;
      if (!force && monthIncomeEntries[id]) continue;
      monthIncomeEntries[id] = {
        id,
        date:          dueDate,
        amount:        tpl.amount,            // expected amount
        actualAmount:  null,                   // null until user confirms
        source:        'template_instance',
        templateId:    tpl.id,
        accountId:     tpl.accountId || null,
        notes:         '',
        paidAt:        null,
        createdAt:     _nowIso(),
        createdBy:     _kvEmailHash || null,
        updatedAt:     _nowIso(),
      };
    }
  }
  const incomeChanged = force || (Object.keys(monthIncomeEntries).length !== beforeIncomeKeys.size);

  billInstances[yyyymm] = monthInstances;
  incomeEntries[yyyymm] = monthIncomeEntries;
  // If we materialised income but the month is now empty, clean up
  if (Object.keys(monthIncomeEntries).length === 0) delete incomeEntries[yyyymm];

  if (!already) budgetSettings.materialisedMonths.push(yyyymm);

  // Skip persist when nothing changed — avoids redundant writes/sync churn
  // on every renderBudget call (which happens often).
  const anythingChanged = !already || billsChanged || incomeChanged || migrated;
  if (persist && anythingChanged) {
    await saveBudgetLocal();
    if (typeof saveBudgetAccountsAndIncomeLocal === 'function') {
      await saveBudgetAccountsAndIncomeLocal();
    }
    _syncQueue?.enqueue('Generating bills…');
  }
  return monthInstances;
};

// ── Variable income: averaging from past actuals ───────────────────────────
//
// Returns the projected amount for an income template. If the template is
// variable, averages the last N actual amounts received (paidAt set, actualAmount
// non-null). Falls back to the template's expected amount if no actuals exist.

const _INCOME_AVERAGE_WINDOW = 3;

function getProjectedIncomeAmount(template) {
  if (!template) return 0;
  if (!template.variableAmount) return template.amount || 0;

  // Walk all months in incomeEntries, collect actuals from this template,
  // sorted by date descending, take the most recent N
  const actuals = [];
  for (const yyyymm of Object.keys(incomeEntries || {})) {
    for (const entry of Object.values(incomeEntries[yyyymm])) {
      if (entry.templateId !== template.id) continue;
      if (entry.actualAmount == null) continue;
      if (!entry.paidAt) continue; // not yet confirmed received
      actuals.push({ date: entry.date, amount: entry.actualAmount });
    }
  }
  if (actuals.length === 0) return template.amount || 0;
  actuals.sort((a, b) => b.date.localeCompare(a.date));
  const window = actuals.slice(0, _INCOME_AVERAGE_WINDOW);
  const sum = window.reduce((s, a) => s + a.amount, 0);
  return Math.round((sum / window.length) * 100) / 100;
}

// Mark an income entry as received with a specific actual amount.
// Updates Phase 3's markIncomeEntryReceived behaviour.
markIncomeEntryReceived = async function(id, actualAmount = null) {
  const located = _findIncomeEntry(id);
  if (!located) return null;
  const expected = located.entry.amount;
  const actual = actualAmount != null ? Number(actualAmount) : expected;
  return updateIncomeEntry(id, {
    paidAt:       _nowIso(),
    actualAmount: actual,
  });
};

// Convenience: get the "best estimate" amount for an income entry, accounting
// for whether the user has confirmed actual receipt or not.
function getEffectiveIncomeAmount(entry) {
  if (entry.actualAmount != null) return entry.actualAmount;
  return entry.amount || 0;
}

// ── Projection updates: variable income, multi-instance handling ───────────
//
// Override Phase 3a's _eventsOnDay to use getProjectedIncomeAmount and
// the keyed-by-date instance format.

_eventsOnDay = function(account, dayIso) {
  const events = [];
  const yyyymm = _yyyymmFromString(dayIso);
  const { year, month } = _parseYyyymm(yyyymm);

  // 1. Bill instances already materialised for this day (new keyed-by-date format)
  const bi = billInstances?.[yyyymm] || {};
  const materialisedBillKeysOnDay = new Set(); // tracks billId+dueDate combos
  for (const key of Object.keys(bi)) {
    const inst = bi[key];
    if (inst.dueDate !== dayIso) continue;
    // Phase 5b: saving instances are paper-only — they don't move money
    // out of the account, so they should never be projection events.
    if (inst.kind === 'saving') continue;
    materialisedBillKeysOnDay.add(`${inst.billId}__${inst.dueDate}`);
    if (inst.skipped || inst.paidAt) continue;
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

  // 2. Bill templates that should land on this day but haven't been materialised
  for (const tpl of (bills || [])) {
    if (tpl.archived) continue;
    const dates = getInstanceDatesInMonth(tpl, year, month);
    if (!dates.includes(dayIso)) continue;
    const key = `${tpl.id}__${dayIso}`;
    if (materialisedBillKeysOnDay.has(key)) continue;
    const effectiveAccountId = tpl.accountId || getPrimaryAccount()?.id;
    if (effectiveAccountId !== account.id) continue;
    events.push({
      type:     'bill',
      amount:   -(tpl.amount || 0),
      label:    tpl.name,
      sourceId: tpl.id,
    });
  }

  // 3. Income entries (incl. materialised template instances) on this day
  const ie = incomeEntries?.[yyyymm] || {};
  const materialisedIncomeKeysOnDay = new Set();
  for (const id of Object.keys(ie)) {
    const entry = ie[id];
    if (entry.date !== dayIso) continue;
    if (entry.templateId) {
      materialisedIncomeKeysOnDay.add(`${entry.templateId}__${entry.date}`);
    }
    if (entry.accountId !== account.id) continue;

    // Already received → not a future projection event. The receipt is
    // already in the user's balance (either via direct balance update or
    // via _applyHistoricalEvents catch-up). Without this skip, confirmed
    // income gets double-counted — see bug report May 2026 where pete's
    // confirmed child benefit kept showing as upcoming income on top of
    // the already-updated joint account balance. Mirrors the bill instance
    // skip at section 1 above.
    if (entry.paidAt) continue;

    // Defensive: skip phantom entries from a template that has been
    // deleted or archived. We've already filtered out paid entries above,
    // so this only fires for unpaid template-instances whose template no
    // longer exists — stale ghosts.
    if (entry.templateId) {
      const tpl = getIncomeTemplateById(entry.templateId);
      if (!tpl || tpl.archived) continue;
    }

    // For variable income with no actual yet, fall back to the projected (averaged) amount
    let amt = entry.actualAmount;
    if (amt == null) {
      // If this is a template instance, use averaged projection; else expected
      if (entry.templateId) {
        const tpl = getIncomeTemplateById(entry.templateId);
        amt = tpl ? getProjectedIncomeAmount(tpl) : entry.amount;
      } else {
        amt = entry.amount;
      }
    }
    events.push({
      type:     'income',
      amount:   (amt || 0),
      label:    entry.notes || (entry.templateId ? getIncomeTemplateById(entry.templateId)?.name : null) || 'Income',
      sourceId: entry.id,
    });
  }

  // 4. Income templates that should land on this day but haven't been materialised
  for (const tpl of (incomeTemplates || [])) {
    if (tpl.archived) continue;
    const dates = getInstanceDatesInMonth(tpl, year, month);
    if (!dates.includes(dayIso)) continue;
    const key = `${tpl.id}__${dayIso}`;
    if (materialisedIncomeKeysOnDay.has(key)) continue;
    if (tpl.accountId && tpl.accountId !== account.id) continue;
    if (!tpl.accountId && !account.isPrimary) continue;
    events.push({
      type:     'income',
      amount:   getProjectedIncomeAmount(tpl),
      label:    tpl.name,
      sourceId: tpl.id,
    });
  }

  return events;
};

// ── _applyHistoricalEvents update ──────────────────────────────────────────
// Override Phase 3a's _applyHistoricalEvents to use the new keyed-by-date
// bill instance format and effective income amounts.

_applyHistoricalEvents = function(startBalance, account, fromIso, toIso) {
  let bal = startBalance;
  const months = _enumerateMonths(fromIso, toIso);

  for (const yyyymm of months) {
    // Bill instances paid in this period
    const bi = billInstances?.[yyyymm] || {};
    for (const inst of Object.values(bi)) {
      if (inst.skipped) continue;
      const tplAccountId = bills.find(b => b.id === inst.billId)?.accountId || getPrimaryAccount()?.id;
      if (tplAccountId !== account.id) continue;
      const eventDate = (inst.paidAt ? inst.paidAt.slice(0, 10) : inst.dueDate);
      if (eventDate <= fromIso || eventDate > toIso) continue;
      bal -= (inst.actualAmount ?? inst.expectedAmount) || 0;
    }

    // Income entries (incl. template instances) received in this period
    const ie = incomeEntries?.[yyyymm] || {};
    for (const entry of Object.values(ie)) {
      if (entry.accountId !== account.id) continue;
      const eventDate = entry.paidAt ? entry.paidAt.slice(0, 10) : entry.date;
      if (eventDate <= fromIso || eventDate > toIso) continue;
      // If actualAmount is set, use it. Otherwise: for past-due unconfirmed entries,
      // we use the EXPECTED amount (assume it landed). For future, we use projection
      // (but those won't be in the catch-up window anyway).
      bal += getEffectiveIncomeAmount(entry);
    }

    // Discretionary transactions (only affect primary)
    if (account.isPrimary) {
      const txs = transactions?.[yyyymm] || {};
      for (const tx of Object.values(txs)) {
        if (tx.date <= fromIso || tx.date > toIso) continue;
        bal -= (tx.amount || 0);
      }
    }
  }

  return bal;
};

// ── Diagnostics ────────────────────────────────────────────────────────────
if (typeof window !== 'undefined') {
  window.budgetPhase4Diag = function () {
    return {
      billsByFrequency: bills.reduce((acc, b) => {
        const f = b.frequency || {};
        const key = `${f.unit || 'month'}:${f.interval || 1}`;
        acc[key] = (acc[key] || 0) + 1;
        return acc;
      }, {}),
      incomeTemplatesByFrequency: incomeTemplates.reduce((acc, t) => {
        const f = t.frequency || {};
        const key = `${f.unit || 'month'}:${f.interval || 1}`;
        acc[key] = (acc[key] || 0) + 1;
        return acc;
      }, {}),
      variableIncomeTemplates: incomeTemplates.filter(t => t.variableAmount).length,
      incomeEntriesWithActuals: Object.values(incomeEntries).reduce(
        (acc, m) => acc + Object.values(m).filter(e => e.actualAmount != null).length, 0
      ),
    };
  };
}


// ═══════════════════════════════════════════════════════════════════════════
//  BUDGET — Phase 4b UI
//  - Bill editor: expanded frequency presets (daily/weekly/4-weekly)
//  - Income template editor: same plus variableAmount toggle
//  - Mark income received modal (mirrors mark-bill-paid for variable income)
//  - Dashboard tile period toggle + bug fix
//
//  Insertion point: in app.js, IMMEDIATELY AFTER the Phase 4a foundations
//  block (just before the GROCERY LIST section).
// ═══════════════════════════════════════════════════════════════════════════

// ── Dashboard tile period state ────────────────────────────────────────────
let _dashboardTilePeriod = 'month';   // 'week' | 'month' — default month per design
let _markIncomeContext   = null;       // { entryId, expected }

// ── Override openBillEditor to populate the Phase 4 frequency presets ──────
const _phase3OpenBillEditor = openBillEditor;
openBillEditor = function(billId = null) {
  // Call the Phase 3 wrap (which itself wraps Phase 1's original)
  _phase3OpenBillEditor.call(this, billId);
  // Now patch the frequency dropdown for Phase 4 unit support
  setTimeout(() => {
    const tpl = billId ? bills.find(b => b.id === billId) : null;
    const sel = document.getElementById('bill-frequency-preset');
    if (!sel) return;
    // Only rebuild the options once (idempotent)
    if (!sel.dataset.phase4) {
      sel.innerHTML = `
        <option value="daily">Daily</option>
        <option value="weekly">Weekly</option>
        <option value="fortnightly">Fortnightly</option>
        <option value="four_weekly">Every 4 weeks</option>
        <option value="monthly">Monthly</option>
        <option value="quarterly">Every 3 months</option>
        <option value="six_monthly">Every 6 months</option>
        <option value="annual">Annually</option>
        <option value="custom">Custom interval…</option>
      `;
      sel.dataset.phase4 = '1';
    }
    // Decide which preset matches the existing template
    const freq = tpl?.frequency || { unit: 'month', interval: 1, anchorMonth: null };
    let preset;
    if      (freq.unit === 'day' && freq.interval === 1)       preset = 'daily';
    else if (freq.unit === 'week' && freq.interval === 1)      preset = 'weekly';
    else if (freq.unit === 'week' && freq.interval === 2)      preset = 'fortnightly';
    else if (freq.unit === 'week' && freq.interval === 4)      preset = 'four_weekly';
    else if (freq.unit === 'month' && freq.interval === 1)     preset = 'monthly';
    else if (freq.unit === 'month' && freq.interval === 3)     preset = 'quarterly';
    else if (freq.unit === 'month' && freq.interval === 6)     preset = 'six_monthly';
    else if (freq.unit === 'year' && freq.interval === 1)      preset = 'annual';
    else                                                        preset = 'custom';
    sel.value = preset;
    // Populate custom unit/interval inputs (in case user picks Custom)
    const customIntInput = document.getElementById('bill-custom-interval');
    const customUnitSel  = document.getElementById('bill-custom-unit');
    if (customIntInput) customIntInput.value = preset === 'custom' ? freq.interval : 2;
    if (customUnitSel)  customUnitSel.value  = preset === 'custom' ? freq.unit     : 'month';
    // Anchor date for weekly/daily templates
    const anchorDateInput = document.getElementById('bill-anchor-date');
    if (anchorDateInput) {
      anchorDateInput.value = freq.anchorDate || (new Date().toISOString().slice(0, 10));
    }
    billOnFreqPresetChange();
  }, 0);
};

// Override billOnFreqPresetChange to handle daily/weekly cases
billOnFreqPresetChange = function() {
  const preset      = document.getElementById('bill-frequency-preset').value;
  const anchorRow   = document.getElementById('bill-anchor-row');
  const anchorDateRow = document.getElementById('bill-anchor-date-row');
  const customRow   = document.getElementById('bill-custom-interval-row');
  const dayOfMonthRow = document.getElementById('bill-day-of-month-row');
  const anchorLabel = document.getElementById('bill-anchor-label');
  const anchorHint  = document.getElementById('bill-anchor-hint');

  // What gets shown for each preset:
  //   daily/weekly/fortnightly/four_weekly  → anchor date, no day-of-month, no anchor month
  //   monthly                                → no anchor month, day-of-month
  //   quarterly/six_monthly                  → anchor month, day-of-month
  //   annual                                 → anchor month, day-of-month
  //   custom                                 → custom unit/interval, plus context-appropriate anchor

  const isWeeklyish = ['daily','weekly','fortnightly','four_weekly'].includes(preset);
  const isCustom    = preset === 'custom';
  const customUnit  = isCustom ? (document.getElementById('bill-custom-unit')?.value || 'month') : null;
  const customIsWeeklyish = isCustom && (customUnit === 'day' || customUnit === 'week');

  // Show anchor-date row for weekly-ish frequencies
  if (anchorDateRow) {
    anchorDateRow.style.display = (isWeeklyish || customIsWeeklyish) ? 'block' : 'none';
  }
  // Show day-of-month input for monthly-ish (everything except weekly-ish)
  if (dayOfMonthRow) {
    dayOfMonthRow.style.display = (isWeeklyish || customIsWeeklyish) ? 'none' : 'block';
  }
  // Anchor month: only shown for non-monthly month-based presets and custom-month
  const showAnchorMonth = ['quarterly','six_monthly','annual'].includes(preset) || (isCustom && customUnit === 'month');
  anchorRow.style.display = showAnchorMonth ? 'block' : 'none';
  customRow.style.display = isCustom ? 'block' : 'none';

  if (showAnchorMonth) {
    if (preset === 'annual' || (isCustom && customUnit === 'year')) {
      anchorLabel.textContent = 'Month it pays';
      anchorHint.textContent  = 'The month the bill leaves your account each year.';
    } else {
      anchorLabel.textContent = 'First payment month';
      const intervalText = preset === 'quarterly'   ? 'every 3 months'
                        : preset === 'six_monthly'  ? 'every 6 months'
                        : 'every N months';
      anchorHint.textContent  = `The first month the bill leaves your account. Then ${intervalText} after that.`;
    }
  }

  // Phase 5: refresh the split-payment section visibility too — without
  // this, switching from quarterly back to monthly leaves the strategy
  // section visible (and vice versa).
  if (typeof _refreshBillSplitVisibility === 'function') _refreshBillSplitVisibility();
};

// Override saveBillFromEditor to translate the new presets into frequency objects
const _phase3SaveBillFromEditor = saveBillFromEditor;
saveBillFromEditor = async function() {
  const name = (document.getElementById('bill-name').value || '').trim();
  if (!name) { toast('Bill needs a name'); return; }
  const amount = parseFloat(document.getElementById('bill-amount').value);
  if (isNaN(amount) || amount < 0) { toast('Enter a valid amount'); return; }
  const variableAmount = document.getElementById('bill-variable').checked;
  const notes = (document.getElementById('bill-notes').value || '').trim();
  const preset = document.getElementById('bill-frequency-preset').value;

  // Build the frequency object based on preset
  let frequency;
  let dayOfMonth = parseInt(document.getElementById('bill-day-of-month').value, 10);
  if (isNaN(dayOfMonth)) dayOfMonth = 1;
  const anchorMonth = parseInt(document.getElementById('bill-anchor-month').value, 10);
  const anchorDate  = document.getElementById('bill-anchor-date')?.value;
  const customUnit  = document.getElementById('bill-custom-unit')?.value || 'month';
  const customInt   = parseInt(document.getElementById('bill-custom-interval').value, 10);

  if (preset === 'daily')           frequency = { unit: 'day',   interval: 1, anchorDate };
  else if (preset === 'weekly')     frequency = { unit: 'week',  interval: 1, anchorDate };
  else if (preset === 'fortnightly') frequency = { unit: 'week', interval: 2, anchorDate };
  else if (preset === 'four_weekly') frequency = { unit: 'week', interval: 4, anchorDate };
  else if (preset === 'monthly')    frequency = { unit: 'month', interval: 1, anchorMonth: null };
  else if (preset === 'quarterly')  frequency = { unit: 'month', interval: 3, anchorMonth };
  else if (preset === 'six_monthly') frequency = { unit: 'month', interval: 6, anchorMonth };
  else if (preset === 'annual')     frequency = { unit: 'year',  interval: 1, anchorMonth };
  else { // custom
    const interval = Math.max(1, customInt || 2);
    if (customUnit === 'day' || customUnit === 'week') {
      frequency = { unit: customUnit, interval, anchorDate };
    } else if (customUnit === 'year') {
      frequency = { unit: 'year', interval, anchorMonth };
    } else {
      frequency = { unit: 'month', interval, anchorMonth };
    }
  }

  // Validate anchor date for weekly/daily templates
  if ((frequency.unit === 'day' || frequency.unit === 'week') && !frequency.anchorDate) {
    toast('Pick an anchor date (the first occurrence)');
    return;
  }

  // For weekly/daily templates dayOfMonth is irrelevant; for others, validate
  if (frequency.unit === 'month' || frequency.unit === 'year') {
    if (dayOfMonth < 1 || dayOfMonth > 31) { toast('Day must be 1-31'); return; }
  }

  // Phase 5: payment strategy. splitInto is auto-derived from the frequency
  // — quarterly → 3 months, six-monthly → 6, annual → 12, etc. The user
  // doesn't pick the split count, which keeps the cycle math consistent.
  let paymentStrategy = 'lump';
  let splitInto       = null;
  if (typeof _billCanSplit === 'function' && _billCanSplit(frequency)) {
    const stratEl = document.querySelector('input[name="bill-payment-strategy"]:checked');
    paymentStrategy = stratEl?.value === 'split' ? 'split' : 'lump';
    if (paymentStrategy === 'split') {
      splitInto = _suggestSplitInto(frequency);
      if (!splitInto || splitInto.count < 1) {
        toast('Cannot split this bill — frequency too short');
        return;
      }
    }
  }

  const patch = { name, amount, variableAmount, dayOfMonth, notes, frequency, paymentStrategy, splitInto };

  let savedBill;
  if (_budgetEditingBillId) {
    savedBill = await updateBill(_budgetEditingBillId, patch);
    toast('Bill updated');
  } else {
    savedBill = await createBill(patch);
    toast('Bill added');
  }

  // Phase 5: if the bill is split-strategy, backfill paid saving instances
  // for past months in the current cycle. Idempotent (skips months that
  // already have an instance). User stays in control of the current month.
  if (savedBill && savedBill.paymentStrategy === 'split') {
    const backfilled = await _backfillSavingInstancesForBill(savedBill);
    if (backfilled > 0) {
      toast(`${backfilled} prior saving month${backfilled === 1 ? '' : 's'} credited`);
    }
  }

  await materialiseMonth(_budgetViewMonth, { persist: true });
  closeModal('bill-editor-modal');
  _budgetEditingBillId = null;
  await renderBudget();
};

// The Phase 3 saveBillFromEditor wrap added accountId handling. Phase 4 replaces
// the whole saveBillFromEditor, so we need to also handle accountId here.
// Snapshot the original for clarity, then re-implement with accountId.
const _phase4SaveBillFromEditorBase = saveBillFromEditor;
saveBillFromEditor = async function() {
  // Read accountId BEFORE the save (which closes the modal & resets state)
  const accSel = document.getElementById('bill-account');
  const accountId = accSel ? (accSel.value || null) : null;
  const editingId = _budgetEditingBillId;
  const billCountBefore = bills.length;
  await _phase4SaveBillFromEditorBase.call(this);
  if (!accountId) return;
  if (editingId) {
    await updateBill(editingId, { accountId });
  } else if (bills.length > billCountBefore) {
    const last = bills[bills.length - 1];
    if (last) await updateBill(last.id, { accountId });
  }
};

// ── Bill amount-change scope prompt ────────────────────────────────────────
//
// When editing an existing bill and changing only its `amount`, the user
// is asked whether the change should apply to the current month (and all
// future months) or only to future months. This wraps the existing
// saveBillFromEditor: if the bill is new, or the amount is unchanged, the
// wrapped function runs immediately as before. If amount changed on an
// existing bill, the save is deferred and a small modal asks for scope.
// The chosen scope is then applied by walking already-materialised
// instances and updating expectedAmount on those that were still tracking
// the template (i.e. not manually overridden, not paid, not skipped).
//
// Saving-kind instances (split bills) are also updated proportionally so
// the per-period amount stays consistent with the new template.

let _billAmountEditCtx = null;

const _phase5cSaveBillFromEditorBase = saveBillFromEditor;
saveBillFromEditor = async function() {
  // Only intercept when editing an existing bill
  if (!_budgetEditingBillId) {
    return _phase5cSaveBillFromEditorBase.call(this);
  }
  const existing = bills.find(b => b.id === _budgetEditingBillId);
  if (!existing) {
    return _phase5cSaveBillFromEditorBase.call(this);
  }
  // Read the proposed amount from the editor input
  const newAmountRaw = parseFloat(document.getElementById('bill-amount').value);
  if (isNaN(newAmountRaw) || newAmountRaw < 0) {
    // Let the base implementation handle the toast/validation
    return _phase5cSaveBillFromEditorBase.call(this);
  }
  const oldAmount = Number(existing.amount) || 0;
  const newAmount = Number(newAmountRaw) || 0;
  // If amount is effectively unchanged, no prompt needed
  if (Math.abs(oldAmount - newAmount) < 0.005) {
    return _phase5cSaveBillFromEditorBase.call(this);
  }
  // Capture context, show the scope modal. The actual save fires when
  // the user picks an option (or cancels).
  _billAmountEditCtx = {
    billId: _budgetEditingBillId,
    oldAmount,
    newAmount,
    name: existing.name || 'this bill',
  };
  document.getElementById('bas-subtitle').textContent =
    `"${_billAmountEditCtx.name}" amount has changed.`;
  document.getElementById('bas-old-amount').textContent = _money(oldAmount);
  document.getElementById('bas-new-amount').textContent = _money(newAmount);
  openModal('bill-amount-scope-modal');
};

// User cancelled the scope choice — close the prompt and leave the bill
// editor open so they can adjust or cancel.
function _cancelBillEditScope() {
  _billAmountEditCtx = null;
  closeModal('bill-amount-scope-modal');
}

// Called by the two scope buttons in the modal. scope is one of:
//   'current_and_future' — apply to current month and all future months
//   'future_only'        — apply to future months only
async function _finaliseBillEditWithScope(scope) {
  const ctx = _billAmountEditCtx;
  if (!ctx) { closeModal('bill-amount-scope-modal'); return; }
  closeModal('bill-amount-scope-modal');
  _billAmountEditCtx = null;
  // Run the real save first — this updates the template and
  // materialises the current view month. Pre-existing instances keep
  // their old expectedAmount because materialiseMonth skips existing keys.
  await _phase5cSaveBillFromEditorBase.call(this);
  // Now propagate to existing instances per scope
  const fromYyyymm = (scope === 'current_and_future')
    ? _yyyymm(new Date())
    : _nextYyyymm(_yyyymm(new Date()));
  const touched = _applyAmountChangeToInstances(
    ctx.billId, ctx.oldAmount, ctx.newAmount, fromYyyymm
  );
  if (touched > 0) {
    await saveBudgetLocal();
    _syncQueue?.enqueue();
    if (_currentView === 'budget') await renderBudget();
  }
}

// Helper: returns YYYY-MM string of the month after the given one.
function _nextYyyymm(yyyymm) {
  const { year, month } = _parseYyyymm(yyyymm);
  let y = year, m = month + 1;
  if (m > 11) { m = 0; y++; }
  return `${y}-${String(m + 1).padStart(2, '0')}`;
}

// Walks billInstances and updates expectedAmount on instances of this
// bill that fall in fromYyyymm or later, where:
//   - paidAt is null
//   - skipped is false
//   - the existing expectedAmount still matches the OLD template amount
//     (i.e. has not been manually overridden)
// Split-saving instances ('kind: saving') are updated proportionally so
// the per-period figure follows the new template.
// Returns the number of instances touched.
function _applyAmountChangeToInstances(billId, oldAmount, newAmount, fromYyyymm) {
  const tpl = bills.find(b => b.id === billId);
  if (!tpl) return 0;
  const splitCount = (tpl.paymentStrategy === 'split' && tpl.splitInto && tpl.splitInto.count)
    ? tpl.splitInto.count : null;
  const oldPerPeriod = splitCount ? Math.round((oldAmount / splitCount) * 100) / 100 : null;
  const newPerPeriod = splitCount ? Math.round((newAmount / splitCount) * 100) / 100 : null;
  let touched = 0;
  for (const yyyymm of Object.keys(billInstances)) {
    if (yyyymm < fromYyyymm) continue;
    const month = billInstances[yyyymm];
    if (!month) continue;
    for (const key of Object.keys(month)) {
      const inst = month[key];
      if (!inst || inst.billId !== billId) continue;
      if (inst.paidAt || inst.skipped) continue;
      if (inst.kind === 'saving') {
        // Saving instances are paid-when-backfilled, but defensive:
        // only touch unpaid ones (above guard already ensures this).
        if (oldPerPeriod == null || newPerPeriod == null) continue;
        if (inst.expectedAmount == null) continue;
        if (Math.abs(inst.expectedAmount - oldPerPeriod) > 0.01) continue; // overridden
        inst.expectedAmount = newPerPeriod;
        inst.updatedAt = _nowIso();
        touched++;
      } else {
        if (inst.expectedAmount == null) continue;
        if (Math.abs(inst.expectedAmount - oldAmount) > 0.01) continue; // overridden
        inst.expectedAmount = newAmount;
        inst.updatedAt = _nowIso();
        touched++;
      }
    }
  }
  return touched;
}

// ── Income template editor — extended for Phase 4 ──────────────────────────
const _phase3OpenIncomeTemplateEditor = openIncomeTemplateEditor;
openIncomeTemplateEditor = function(id = null) {
  _phase3OpenIncomeTemplateEditor.call(this, id);
  // Patch the frequency dropdown
  setTimeout(() => {
    const tpl = id ? getIncomeTemplateById(id) : null;
    const sel = document.getElementById('income-tpl-frequency');
    if (!sel) return;
    if (!sel.dataset.phase4) {
      sel.innerHTML = `
        <option value="weekly">Weekly</option>
        <option value="fortnightly">Fortnightly</option>
        <option value="four_weekly">Every 4 weeks</option>
        <option value="monthly">Monthly</option>
        <option value="quarterly">Every 3 months</option>
        <option value="six_monthly">Every 6 months</option>
        <option value="annual">Annually</option>
        <option value="custom">Custom interval…</option>
      `;
      sel.dataset.phase4 = '1';
    }
    const freq = tpl?.frequency || { unit: 'month', interval: 1, anchorMonth: null };
    let preset;
    if      (freq.unit === 'week'  && freq.interval === 1) preset = 'weekly';
    else if (freq.unit === 'week'  && freq.interval === 2) preset = 'fortnightly';
    else if (freq.unit === 'week'  && freq.interval === 4) preset = 'four_weekly';
    else if (freq.unit === 'month' && freq.interval === 1) preset = 'monthly';
    else if (freq.unit === 'month' && freq.interval === 3) preset = 'quarterly';
    else if (freq.unit === 'month' && freq.interval === 6) preset = 'six_monthly';
    else if (freq.unit === 'year'  && freq.interval === 1) preset = 'annual';
    else                                                    preset = 'custom';
    sel.value = preset;
    const customIntInput = document.getElementById('income-tpl-custom-interval');
    const customUnitSel  = document.getElementById('income-tpl-custom-unit');
    if (customIntInput) customIntInput.value = preset === 'custom' ? freq.interval : 2;
    if (customUnitSel)  customUnitSel.value  = preset === 'custom' ? freq.unit     : 'month';
    const anchorDateInput = document.getElementById('income-tpl-anchor-date');
    if (anchorDateInput) {
      anchorDateInput.value = freq.anchorDate || (new Date().toISOString().slice(0, 10));
    }
    // Variable amount checkbox
    const variableCheck = document.getElementById('income-tpl-variable');
    if (variableCheck) variableCheck.checked = !!tpl?.variableAmount;
    incomeTplFreqChanged();
  }, 0);
};

incomeTplFreqChanged = function() {
  const preset      = document.getElementById('income-tpl-frequency').value;
  const anchorRow   = document.getElementById('income-tpl-anchor-row');
  const anchorDateRow = document.getElementById('income-tpl-anchor-date-row');
  const customRow   = document.getElementById('income-tpl-custom-row');
  const dayRow      = document.getElementById('income-tpl-day-row');

  const isWeeklyish = ['weekly','fortnightly','four_weekly'].includes(preset);
  const isCustom    = preset === 'custom';
  const customUnit  = isCustom ? (document.getElementById('income-tpl-custom-unit')?.value || 'month') : null;
  const customIsWeeklyish = isCustom && (customUnit === 'day' || customUnit === 'week');

  if (anchorDateRow) anchorDateRow.style.display = (isWeeklyish || customIsWeeklyish) ? 'block' : 'none';
  if (dayRow)        dayRow.style.display        = (isWeeklyish || customIsWeeklyish) ? 'none'  : 'block';
  const showAnchorMonth = ['quarterly','six_monthly','annual'].includes(preset) || (isCustom && customUnit === 'month');
  if (anchorRow) anchorRow.style.display = showAnchorMonth ? 'block' : 'none';
  if (customRow) customRow.style.display = isCustom ? 'block' : 'none';
};

const _phase3SaveIncomeTemplateFromEditor = saveIncomeTemplateFromEditor;
saveIncomeTemplateFromEditor = async function() {
  const name = (document.getElementById('income-tpl-name').value || '').trim();
  if (!name) { toast('Income needs a name'); return; }
  const amount = parseFloat(document.getElementById('income-tpl-amount').value);
  if (isNaN(amount) || amount <= 0) { toast('Enter a valid amount'); return; }
  const notes = (document.getElementById('income-tpl-notes').value || '').trim();
  const accountId = document.getElementById('income-tpl-account').value || null;
  const variableAmount = document.getElementById('income-tpl-variable')?.checked || false;

  const preset      = document.getElementById('income-tpl-frequency').value;
  const anchorMonth = parseInt(document.getElementById('income-tpl-anchor-month').value, 10);
  const anchorDate  = document.getElementById('income-tpl-anchor-date')?.value;
  const customUnit  = document.getElementById('income-tpl-custom-unit')?.value || 'month';
  const customInt   = parseInt(document.getElementById('income-tpl-custom-interval').value, 10);
  let dayOfMonth    = parseInt(document.getElementById('income-tpl-day').value, 10);
  if (isNaN(dayOfMonth)) dayOfMonth = 25;

  let frequency;
  if (preset === 'weekly')      frequency = { unit: 'week',  interval: 1, anchorDate };
  else if (preset === 'fortnightly') frequency = { unit: 'week', interval: 2, anchorDate };
  else if (preset === 'four_weekly') frequency = { unit: 'week', interval: 4, anchorDate };
  else if (preset === 'monthly') frequency = { unit: 'month', interval: 1, anchorMonth: null };
  else if (preset === 'quarterly') frequency = { unit: 'month', interval: 3, anchorMonth };
  else if (preset === 'six_monthly') frequency = { unit: 'month', interval: 6, anchorMonth };
  else if (preset === 'annual')  frequency = { unit: 'year', interval: 1, anchorMonth };
  else { // custom
    const interval = Math.max(1, customInt || 2);
    if (customUnit === 'day' || customUnit === 'week') {
      frequency = { unit: customUnit, interval, anchorDate };
    } else if (customUnit === 'year') {
      frequency = { unit: 'year', interval, anchorMonth };
    } else {
      frequency = { unit: 'month', interval, anchorMonth };
    }
  }

  if ((frequency.unit === 'day' || frequency.unit === 'week') && !frequency.anchorDate) {
    toast('Pick an anchor date (the first occurrence)');
    return;
  }
  if (frequency.unit === 'month' || frequency.unit === 'year') {
    if (dayOfMonth < 1 || dayOfMonth > 31) { toast('Day must be 1-31'); return; }
  }

  const patch = { name, amount, variableAmount, dayOfMonth, notes, accountId, frequency };

  if (_incomeTplEditingId) {
    await updateIncomeTemplate(_incomeTplEditingId, patch);
    toast('Income updated');
  } else {
    await createIncomeTemplate(patch);
    toast('Income added');
  }
  // Re-materialise to pick up new instances
  await materialiseMonth(_budgetViewMonth, { persist: true });
  closeModal('income-tpl-modal');
  _incomeTplEditingId = null;
  renderBudgetAccounts();
  if (_currentView === 'budget') renderBudget();
};

// ── Mark income received modal ─────────────────────────────────────────────
//
// Replaces the simpler markIncomeEntryReceived call with a modal that prompts
// for the actual amount when the entry's template is variable.
function handleMarkIncomeReceived(entryId) {
  const located = _findIncomeEntry(entryId);
  if (!located) return;
  const entry = located.entry;
  const tpl   = entry.templateId ? getIncomeTemplateById(entry.templateId) : null;
  // For variable income, prompt for actual amount
  if (tpl && tpl.variableAmount) {
    _markIncomeContext = { entryId, expected: entry.amount };
    document.getElementById('mki-subtitle').textContent = `${tpl.name} — expected ${_shortDate(entry.date)}`;
    const amtIn = document.getElementById('mki-amount');
    amtIn.value = entry.actualAmount != null ? entry.actualAmount : entry.amount;
    document.getElementById('mki-expected-hint').textContent = `Estimated: ${_money(entry.amount)}`;
    openModal('mark-income-modal');
    setTimeout(() => amtIn.select(), 50);
    return;
  }
  // Fixed amount — confirm with one tap
  _confirmMarkIncomeFixed(entryId, entry.amount);
}

async function _confirmMarkIncomeFixed(entryId, expected) {
  await markIncomeEntryReceived(entryId, expected);
  toast('Income confirmed');
  if (_currentView === 'budget') {
    if (_budgetActivePanel === 'accounts')   renderBudgetAccounts();
    else if (_budgetActivePanel === 'dashboard') renderBudgetDashboard();
  }
}

async function confirmMarkIncomeReceived() {
  const ctx = _markIncomeContext;
  if (!ctx) return;
  const amt = parseFloat(document.getElementById('mki-amount').value);
  if (isNaN(amt) || amt < 0) { toast('Enter a valid amount'); return; }
  await markIncomeEntryReceived(ctx.entryId, amt);
  closeModal('mark-income-modal');
  _markIncomeContext = null;
  toast('Income confirmed');
  if (_currentView === 'budget') {
    if (_budgetActivePanel === 'accounts')   renderBudgetAccounts();
    else if (_budgetActivePanel === 'dashboard') renderBudgetDashboard();
  }
}

// Override Phase 3's _renderIncomeEntriesList & _renderIncomeTemplatesList
// to surface Mark Received when the entry is upcoming/unconfirmed.
// Also include materialised template instances (not just one-off entries).
const _phase3RenderIncomeEntriesList = _renderIncomeEntriesList;
_renderIncomeEntriesList = function() {
  const section = document.getElementById('budget-income-entries-section');
  const host    = document.getElementById('budget-income-entries-list');
  if (!section || !host) return;

  // Show last 60 days of all income entries (one-off + materialised template instances)
  const today = new Date();
  const past  = new Date(today.getTime() - 60 * MS_PER_DAY);
  const future = new Date(today.getTime() + 14 * MS_PER_DAY); // include next 2 weeks
  const startIso = past.toISOString().slice(0, 10);
  const endIso   = future.toISOString().slice(0, 10);
  const todayIso = today.toISOString().slice(0, 10);

  const entries = getIncomeEntriesForRange(startIso, endIso)
    .sort((a, b) => b.date.localeCompare(a.date));

  if (entries.length === 0) {
    host.innerHTML = `
      <div style="padding:14px;text-align:center;color:var(--muted);font-size:12px">
        No income entries in this period.
        <button class="btn btn-ghost btn-sm" onclick="openIncomeEntryEditor()" style="margin-top:8px;font-size:11px"><svg class="icon" aria-hidden="true"><use href="#i-plus"></use></svg> Add bonus / refund / gift</button>
      </div>`;
    return;
  }
  host.innerHTML = `<div class="bill-list">${entries.map(e => _renderIncomeEntryRowPhase4(e, todayIso)).join('')}</div>`;
};

function _renderIncomeEntryRowPhase4(entry, todayIso) {
  const acc = getAccountById(entry.accountId);
  const accLabel = acc ? acc.name : '(no account)';
  const dayLabel = (() => {
    const d = new Date(entry.date + 'T12:00:00');
    return d.toLocaleDateString(undefined, { day: 'numeric', month: 'short' });
  })();
  const isFuture     = entry.date > todayIso;
  const isReceived   = !!entry.paidAt;
  const isOverdue    = !isReceived && !isFuture && entry.date < todayIso;
  const isFromTemplate = !!entry.templateId;

  // Determine amount to display: actual if confirmed, expected (or projected) otherwise
  let displayAmount = entry.actualAmount != null ? entry.actualAmount : entry.amount;
  if (!isReceived && entry.templateId) {
    const tpl = getIncomeTemplateById(entry.templateId);
    if (tpl && tpl.variableAmount) {
      displayAmount = getProjectedIncomeAmount(tpl);
    }
  }

  const stateClass = isReceived ? 'is-paid' : (isOverdue ? 'is-overdue' : '');
  let stateLabel = '';
  if (isReceived)      stateLabel = `<span style="color:var(--ok);font-size:10px">RECEIVED</span>`;
  else if (isOverdue)  stateLabel = `<span style="color:var(--danger);font-size:10px">OVERDUE</span>`;
  else if (isFuture)   stateLabel = `<span style="color:var(--muted);font-size:10px">UPCOMING</span>`;

  // Variable income tag if applicable
  const tpl = entry.templateId ? getIncomeTemplateById(entry.templateId) : null;
  const variableTag = (tpl && tpl.variableAmount && !isReceived)
    ? `<span class="bill-tag" style="background:rgba(91,141,238,0.12);color:var(--accent2);border-color:rgba(91,141,238,0.3);font-size:9px">est.</span>`
    : '';

  // Action buttons
  let actions = '';
  if (!isReceived) {
    actions += `<button class="bill-action-btn bill-action-paid" onclick="event.stopPropagation();handleMarkIncomeReceived('${entry.id}')" title="Mark received"><svg aria-hidden="true"><use href="#i-check"></use></svg></button>`;
  } else {
    actions += `<button class="bill-action-btn" onclick="event.stopPropagation();handleUnmarkIncomeReceived('${entry.id}')" title="Unmark received"><svg aria-hidden="true"><use href="#i-refresh-cw"></use></svg></button>`;
  }
  actions += `<button class="bill-action-btn bill-action-edit" onclick="event.stopPropagation();openIncomeEntryEditor('${entry.id}')" title="Edit"><svg aria-hidden="true"><use href="#i-pencil"></use></svg></button>`;

  const name = entry.notes || (tpl ? tpl.name : 'Income');
  return `
    <div class="bill-row ${stateClass}" onclick="openIncomeEntryEditor('${entry.id}')" style="cursor:pointer">
      <div class="bill-day" style="background:rgba(76,187,138,0.15);color:var(--ok);border-color:rgba(76,187,138,0.4);font-size:10px">${entry.date.slice(8, 10)}</div>
      <div class="bill-info">
        <div class="bill-name">${_escapeHtml(name)} ${variableTag}</div>
        <div class="bill-meta">${dayLabel} · ${_escapeHtml(accLabel)} · ${stateLabel}</div>
      </div>
      <div class="bill-amount" style="color:${isReceived ? 'var(--ok)' : 'var(--text)'}">+${_money(displayAmount)}</div>
      <div class="bill-actions">${actions}</div>
    </div>`;
}

async function handleUnmarkIncomeReceived(entryId) {
  await updateIncomeEntry(entryId, { paidAt: null, actualAmount: null });
  toast('Income unmarked');
  if (_currentView === 'budget') {
    if (_budgetActivePanel === 'accounts')   renderBudgetAccounts();
    else if (_budgetActivePanel === 'dashboard') renderBudgetDashboard();
  }
}

// ── Dashboard tile period toggle + bug fix ─────────────────────────────────
//
// Phase 1's renderBudgetDashboard used `_budgetViewMonth` for tile aggregation
// (= the month controlled by the bill chevrons). That's wrong for spend tiles
// since the user expects "current period" regardless of which bills they're
// browsing. Phase 4b anchors tiles to today, with a Week/Month toggle.

function setDashboardTilePeriod(period) {
  if (period !== 'week' && period !== 'month') return;
  _dashboardTilePeriod = period;
  // Update toggle visual
  document.querySelectorAll('.dashboard-tile-period-btn').forEach(btn => {
    const active = btn.dataset.period === period;
    btn.classList.toggle('active', active);
    btn.style.background = active ? 'var(--surface)' : 'transparent';
    btn.style.color      = active ? 'var(--text)'    : 'var(--muted)';
  });
  // Re-render tiles only
  if (_currentView === 'budget' && _budgetActivePanel === 'dashboard') {
    _renderDashboardTiles();
  }
}

function _renderDashboardTiles() {
  const tilesHost  = document.getElementById('budget-dashboard-tiles');
  const tilesEmpty = document.getElementById('budget-dashboard-tiles-empty');
  const tilesWrap  = document.getElementById('budget-dashboard-tiles-wrap');
  if (!tilesHost || !tilesEmpty) return;

  const cats = getActiveBudgetCategories();
  if (cats.length === 0) {
    if (tilesWrap)  tilesWrap.style.display = 'none';
    tilesHost.style.display = 'none';
    tilesEmpty.style.display = 'block';
    return;
  }
  if (tilesWrap)  tilesWrap.style.display = 'block';
  tilesEmpty.style.display = 'none';
  tilesHost.style.display = 'flex';

  // Anchor to TODAY's period — not the bill view's month. This is the bug fix:
  // dashboard tiles should always reflect "what have I spent recently" rather
  // than tracking the bill chevrons.
  const today = new Date();
  let startIso, endIso;
  if (_dashboardTilePeriod === 'week') {
    const wk = getWeekRange(today, budgetSettings.weekStart || 'mon');
    startIso = wk.startIso;
    endIso   = wk.endIso;
  } else {
    const y = today.getFullYear(), m = today.getMonth();
    startIso = `${y}-${String(m+1).padStart(2,'0')}-01`;
    const lastDay = new Date(y, m + 1, 0).getDate();
    endIso   = `${y}-${String(m+1).padStart(2,'0')}-${String(lastDay).padStart(2,'0')}`;
  }
  tilesHost.innerHTML = cats.map(cat =>
    _renderCategoryTile(cat, startIso, endIso, _dashboardTilePeriod, false)
  ).join('');
}

// Override the existing renderBudgetDashboard so it uses the new tile logic.
const _phase3RenderBudgetDashboard = renderBudgetDashboard;
renderBudgetDashboard = function() {
  // Run the original (which renders hero, upcoming bills, original tile call).
  // The original uses _budgetViewMonth for tiles — we'll re-render them after
  // with the corrected anchor.
  _phase3RenderBudgetDashboard.call(this);
  // Now overwrite the tiles with the today-anchored version
  _renderDashboardTiles();
};


// ═══════════════════════════════════════════════════════════════════════════
//  BUDGET — Phase 4.5 Polish
//  - Year-over-year category comparison on tiles
//  - CSV export
//  - Variable bill amount history sparkline
//  - Bill instance one-off override
//
//  Insertion point: in app.js, IMMEDIATELY AFTER the Phase 4b BUDGET UI block
//  (just before the GROCERY LIST section).
// ═══════════════════════════════════════════════════════════════════════════

// ── State ──────────────────────────────────────────────────────────────────
let _billOverrideContext   = null;   // { yyyymm, billId, dueDate, original }
let _csvExportRange        = 'last30'; // dropdown state

// ── Year-over-year category comparison ─────────────────────────────────────
//
// Returns { prior: number, delta: number } where:
//   prior = total spent for category in the same period one year ago
//   delta = current - prior (positive = spent more this year)
// Returns null if no transactions in the prior period (don't surface "vs £0").

function getCategoryYoYComparison(categoryId, startIso, endIso) {
  const priorStart = _shiftIsoYear(startIso, -1);
  const priorEnd   = _shiftIsoYear(endIso,   -1);
  const priorSpend = getSpendForCategoryInRange(priorStart, priorEnd, categoryId);
  if (priorSpend === 0) return null;
  const currentSpend = getSpendForCategoryInRange(startIso, endIso, categoryId);
  return {
    prior: Math.round(priorSpend * 100) / 100,
    delta: Math.round((currentSpend - priorSpend) * 100) / 100,
    priorStartIso: priorStart,
    priorEndIso:   priorEnd,
  };
}

function _shiftIsoYear(iso, delta) {
  // 'YYYY-MM-DD' → shifted year. Handles Feb 29 → Feb 28 in non-leap years.
  const y = parseInt(iso.slice(0, 4), 10);
  const m = parseInt(iso.slice(5, 7), 10);
  const d = parseInt(iso.slice(8, 10), 10);
  const newYear = y + delta;
  // Days in the target month/year
  const daysInTarget = new Date(newYear, m, 0).getDate();
  const newDay = Math.min(d, daysInTarget);
  return `${newYear}-${String(m).padStart(2,'0')}-${String(newDay).padStart(2,'0')}`;
}

// Wrap _renderCategoryTile to append a YoY delta line when applicable
const _phase45OrigRenderCategoryTile = _renderCategoryTile;
_renderCategoryTile = function(cat, startIso, endIso, period, clickable) {
  const baseHtml = _phase45OrigRenderCategoryTile.call(this, cat, startIso, endIso, period, clickable);
  // Skip for tiles in the Spend panel (which has its own period nav and doesn't
  // need YoY noise). Heuristic: only show on dashboard tiles, identified by
  // !clickable (Spend panel tiles are clickable for category filter).
  if (clickable) return baseHtml;
  const yoy = getCategoryYoYComparison(cat.id, startIso, endIso);
  if (!yoy) return baseHtml;
  // Build the delta line
  const sign = yoy.delta > 0 ? '+' : '';
  const color = yoy.delta > 5 ? 'var(--danger)'
              : yoy.delta < -5 ? 'var(--ok)'
              : 'var(--muted)';
  const moreOrLess = yoy.delta > 0 ? 'more' : (yoy.delta < 0 ? 'less' : 'same');
  const priorLabel = (() => {
    // Compact label: "vs Apr 24" or "vs May 25" etc.
    const d = new Date(yoy.priorStartIso + 'T12:00:00');
    return d.toLocaleDateString(undefined, { month: 'short', year: '2-digit' });
  })();
  const deltaLine = `
    <div style="font-size:10px;color:${color};font-family:var(--mono);margin-top:4px">
      ${sign}${_money(yoy.delta)} ${moreOrLess} vs ${priorLabel}
    </div>`;
  // Inject just before the closing </div> of the tile (after the bar div)
  return baseHtml.replace(/(<\/div>)(\s*<\/div>)$/, '$1' + deltaLine + '$2');
};

// ── CSV export ─────────────────────────────────────────────────────────────
//
// Exports bills paid, transactions, and income entries within a date range to
// a single CSV file. Three sections concatenated, each with its own header row.

function openCsvExportModal() {
  // Default range: last 30 days
  const today = new Date();
  const past = new Date(today.getTime() - 30 * MS_PER_DAY);
  document.getElementById('csv-export-start').value = past.toISOString().slice(0, 10);
  document.getElementById('csv-export-end').value   = today.toISOString().slice(0, 10);
  _csvExportRange = 'last30';
  csvExportRangeChanged();
  openModal('csv-export-modal');
}

function csvExportRangeChanged() {
  const range = document.getElementById('csv-export-range').value;
  _csvExportRange = range;
  const today = new Date();
  let start, end;
  if (range === 'last30') {
    end   = today;
    start = new Date(today.getTime() - 30 * MS_PER_DAY);
  } else if (range === 'last90') {
    end   = today;
    start = new Date(today.getTime() - 90 * MS_PER_DAY);
  } else if (range === 'this_year') {
    end   = today;
    start = new Date(today.getFullYear(), 0, 1);
  } else if (range === 'last_year') {
    start = new Date(today.getFullYear() - 1, 0, 1);
    end   = new Date(today.getFullYear() - 1, 11, 31);
  } else if (range === 'all_time') {
    start = new Date(2000, 0, 1);
    end   = today;
  } else {
    // 'custom' — leave fields editable
    show('csv-export-custom-row', 'block');
    return;
  }
  document.getElementById('csv-export-start').value = start.toISOString().slice(0, 10);
  document.getElementById('csv-export-end').value   = end.toISOString().slice(0, 10);
  toggle('csv-export-custom-row', (range === 'custom'), 'block');
}

function _csvEscape(value) {
  if (value == null) return '';
  const s = String(value);
  // RFC 4180: wrap in quotes if contains comma, quote, or newline; double internal quotes
  if (/[",\n\r]/.test(s)) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function _buildCsvSection(title, headers, rows) {
  const lines = [];
  lines.push(`# ${title}`);
  lines.push(headers.map(_csvEscape).join(','));
  for (const row of rows) {
    lines.push(row.map(_csvEscape).join(','));
  }
  return lines.join('\n');
}

function generateCsvExport(startIso, endIso) {
  const sections = [];

  // Bills paid section
  const billRows = [];
  for (const yyyymm of Object.keys(billInstances)) {
    for (const inst of Object.values(billInstances[yyyymm])) {
      const eventDate = inst.paidAt ? inst.paidAt.slice(0, 10) : inst.dueDate;
      if (eventDate < startIso || eventDate > endIso) continue;
      if (inst.skipped) continue; // skipped = not really an event
      const tpl = bills.find(b => b.id === inst.billId);
      const amount = inst.actualAmount ?? inst.expectedAmount ?? 0;
      billRows.push([
        inst.dueDate,
        tpl?.name || '(deleted bill)',
        amount.toFixed(2),
        inst.paidAt ? 'paid' : 'expected',
        inst.paidAt ? inst.paidAt.slice(0, 10) : '',
        tpl?.notes || '',
      ]);
    }
  }
  billRows.sort((a, b) => a[0].localeCompare(b[0]));
  sections.push(_buildCsvSection(
    'BILLS',
    ['Due date', 'Name', `Amount (${currencySymbol()||'no currency'})`, 'Status', 'Paid date', 'Notes'],
    billRows,
  ));

  // Transactions section
  const txRows = [];
  for (const yyyymm of Object.keys(transactions || {})) {
    for (const tx of Object.values(transactions[yyyymm])) {
      if (tx.date < startIso || tx.date > endIso) continue;
      const cat = getBudgetCategoryById(tx.categoryId);
      txRows.push([
        tx.date,
        tx.where || '',
        (tx.amount || 0).toFixed(2),
        cat?.name || '',
        tx.notes || '',
      ]);
    }
  }
  txRows.sort((a, b) => a[0].localeCompare(b[0]));
  sections.push(_buildCsvSection(
    'TRANSACTIONS',
    ['Date', 'Where', `Amount (${currencySymbol()||'no currency'})`, 'Category', 'Notes'],
    txRows,
  ));

  // Income entries section
  const incomeRows = [];
  for (const yyyymm of Object.keys(incomeEntries || {})) {
    for (const entry of Object.values(incomeEntries[yyyymm])) {
      const eventDate = entry.paidAt ? entry.paidAt.slice(0, 10) : entry.date;
      if (eventDate < startIso || eventDate > endIso) continue;
      const tpl = entry.templateId ? getIncomeTemplateById(entry.templateId) : null;
      const acc = getAccountById(entry.accountId);
      const amount = entry.actualAmount ?? entry.amount ?? 0;
      incomeRows.push([
        entry.date,
        entry.notes || tpl?.name || 'Income',
        amount.toFixed(2),
        entry.paidAt ? 'received' : 'expected',
        entry.paidAt ? entry.paidAt.slice(0, 10) : '',
        acc?.name || '',
      ]);
    }
  }
  incomeRows.sort((a, b) => a[0].localeCompare(b[0]));
  sections.push(_buildCsvSection(
    'INCOME',
    ['Date', 'Source', `Amount (${currencySymbol()||'no currency'})`, 'Status', 'Received date', 'Account'],
    incomeRows,
  ));

  // Header with metadata
  const header = [
    `# STOCKROOM Budget Export`,
    `# Range: ${startIso} to ${endIso}`,
    `# Generated: ${new Date().toISOString()}`,
    '',
  ].join('\n');

  return header + sections.join('\n\n') + '\n';
}

function confirmCsvExport() {
  const startIso = document.getElementById('csv-export-start').value;
  const endIso   = document.getElementById('csv-export-end').value;
  if (!startIso || !endIso) { toast('Pick a date range'); return; }
  if (startIso > endIso)    { toast('Start must be before end'); return; }

  const csv = generateCsvExport(startIso, endIso);
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
  const url  = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `stockroom-budget-${startIso}-to-${endIso}.csv`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  setTimeout(() => URL.revokeObjectURL(url), 1000);
  closeModal('csv-export-modal');
  toast('CSV downloaded');
}

// ── Variable bill amount history sparkline ─────────────────────────────────
//
// Returns last N actual amounts paid for a given bill id, sorted oldest-first.

function getBillActualHistory(billId, max = 6) {
  const history = [];
  for (const yyyymm of Object.keys(billInstances)) {
    for (const inst of Object.values(billInstances[yyyymm])) {
      if (inst.billId !== billId) continue;
      if (!inst.paidAt) continue;
      if (inst.actualAmount == null) continue;
      history.push({ date: inst.paidAt.slice(0, 10), amount: inst.actualAmount });
    }
  }
  history.sort((a, b) => a.date.localeCompare(b.date));
  return history.slice(-max);
}

function _renderBillSparkline(billId) {
  const history = getBillActualHistory(billId, 6);
  if (history.length < 2) {
    return ''; // need at least 2 points to draw a line
  }
  const W = 240, H = 50;
  const padX = 4, padY = 8;
  const innerW = W - padX * 2;
  const innerH = H - padY * 2;
  const amounts = history.map(h => h.amount);
  const minA = Math.min(...amounts);
  const maxA = Math.max(...amounts);
  const range = (maxA - minA) || 1;
  const mean = amounts.reduce((s, a) => s + a, 0) / amounts.length;

  const xFor = (i) => padX + (i / (history.length - 1)) * innerW;
  const yFor = (a) => padY + (1 - (a - minA) / range) * innerH;

  const linePoints = history.map((h, i) => `${xFor(i).toFixed(1)},${yFor(h.amount).toFixed(1)}`).join(' ');
  const meanY = yFor(mean).toFixed(1);

  const dots = history.map((h, i) => {
    const cx = xFor(i).toFixed(1);
    const cy = yFor(h.amount).toFixed(1);
    return `<circle cx="${cx}" cy="${cy}" r="2.5" fill="var(--accent2)"/>`;
  }).join('');

  const minLabel = `<text x="${padX}" y="${(yFor(minA) - 2).toFixed(1)}" font-size="9" font-family="var(--mono)" fill="var(--muted)">${_money(minA)}</text>`;
  const maxLabel = `<text x="${padX}" y="${(yFor(maxA) + 9).toFixed(1)}" font-size="9" font-family="var(--mono)" fill="var(--muted)">${_money(maxA)}</text>`;

  return `
    <div style="background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:8px 10px;margin-top:8px">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px">
        <span style="font-size:10px;color:var(--muted);font-family:var(--mono);text-transform:uppercase;letter-spacing:0.5px">Last ${history.length} actual${history.length === 1 ? '' : 's'}</span>
        <span style="font-size:10px;color:var(--muted);font-family:var(--mono)">avg ${_money(mean)}</span>
      </div>
      <svg viewBox="0 0 ${W} ${H}" width="100%" height="${H}" aria-hidden="true" style="display:block">
        <line x1="${padX}" y1="${meanY}" x2="${W - padX}" y2="${meanY}" stroke="var(--muted)" stroke-width="0.5" stroke-dasharray="2,3" opacity="0.5"/>
        <polyline points="${linePoints}" fill="none" stroke="var(--accent2)" stroke-width="1.5"/>
        ${dots}
      </svg>
    </div>`;
}

// Wrap openBillEditor (final layer) to render the sparkline when the bill is variable
const _phase45OrigOpenBillEditor = openBillEditor;
openBillEditor = function(billId = null) {
  _phase45OrigOpenBillEditor.call(this, billId);
  setTimeout(() => {
    const sparkHost = document.getElementById('bill-sparkline-host');
    if (!sparkHost) return;
    if (!billId) { sparkHost.innerHTML = ''; sparkHost.style.display = 'none'; return; }
    const tpl = bills.find(b => b.id === billId);
    if (!tpl || !tpl.variableAmount) {
      sparkHost.innerHTML = '';
      sparkHost.style.display = 'none';
      return;
    }
    const html = _renderBillSparkline(billId);
    if (!html) { sparkHost.innerHTML = ''; sparkHost.style.display = 'none'; return; }
    sparkHost.innerHTML = html;
    sparkHost.style.display = 'block';
  }, 10);
};

// ── Bill instance one-off override ─────────────────────────────────────────
//
// Lets user set this month's expectedAmount different from the template, without
// marking paid. Useful when you know in advance: "this month's electricity is £200."

function openBillOverrideModal(yyyymm, billId, dueDate) {
  const inst = _getInstance(yyyymm, billId, dueDate);
  if (!inst) return;
  const tpl  = bills.find(b => b.id === billId);
  if (!tpl) return;
  _billOverrideContext = { yyyymm, billId, dueDate: inst.dueDate, original: tpl.amount };
  document.getElementById('bo-subtitle').textContent =
    `${tpl.name} — due ${_shortDate(inst.dueDate)}`;
  document.getElementById('bo-template-amount').textContent = _money(tpl.amount);
  const amtIn = document.getElementById('bo-amount');
  amtIn.value = inst.expectedAmount ?? tpl.amount;
  openModal('bill-override-modal');
  setTimeout(() => amtIn.select(), 50);
}

async function confirmBillOverride() {
  const ctx = _billOverrideContext;
  if (!ctx) return;
  const amt = parseFloat(document.getElementById('bo-amount').value);
  if (isNaN(amt) || amt < 0) { toast('Enter a valid amount'); return; }
  await _setInstance(ctx.yyyymm, ctx.billId, { expectedAmount: amt }, ctx.dueDate);
  closeModal('bill-override-modal');
  _billOverrideContext = null;
  toast('Amount overridden for this month');
  if (_currentView === 'budget') await renderBudget();
}

async function resetBillOverride() {
  const ctx = _billOverrideContext;
  if (!ctx) return;
  await _setInstance(ctx.yyyymm, ctx.billId, { expectedAmount: ctx.original }, ctx.dueDate);
  closeModal('bill-override-modal');
  _billOverrideContext = null;
  toast('Reset to template amount');
  if (_currentView === 'budget') await renderBudget();
}

// Override _renderBillRow to: (a) add an override-amount button when unpaid,
// (b) mark overridden instances with an OVERRIDE tag.
const _phase45OrigRenderBillRow = _renderBillRow;
_renderBillRow = function(inst, opts = {}) {
  let html = _phase45OrigRenderBillRow.call(this, inst, opts);
  const tpl = bills.find(b => b.id === inst.billId);
  if (!tpl) return html;
  const yyyymm = _yyyymmFromString(inst.dueDate);

  // (a) Inject the override button when unpaid + not skipped
  if (!inst.paidAt && !inst.skipped) {
    const overrideBtn = `<button class="bill-action-btn" onclick="event.stopPropagation();openBillOverrideModal('${yyyymm}','${inst.billId}','${inst.dueDate}')" title="Override amount this month" style="font-size:10px;font-family:var(--mono);font-weight:700">±${currencySymbol()}</button>`;
    // Insert before the existing edit button (matches `bill-action-edit` class)
    html = html.replace(/(<button[^>]*bill-action-btn bill-action-edit[^<]*<\/button>)/, `${overrideBtn}$1`);
  }

  // (b) Mark overridden instances visually
  if (inst.expectedAmount != null && Math.abs(inst.expectedAmount - tpl.amount) > 0.01) {
    const tag = `<span class="bill-tag" style="background:rgba(232,168,56,0.15);color:var(--accent);border-color:rgba(232,168,56,0.3);font-size:9px">OVERRIDE</span>`;
    // Inject just before the closing </div> of the .bill-name div. Use a
    // non-greedy match across nested HTML inside the bill-name (which can
    // contain a variableTag span).
    html = html.replace(/(<div class="bill-name">[\s\S]*?)(<\/div>)/, `$1 ${tag}$2`);
  }
  return html;
};

// ── Diagnostics ────────────────────────────────────────────────────────────
if (typeof window !== 'undefined') {
  window.budgetPhase45Diag = function () {
    const billsWithOverrides = [];
    for (const yyyymm of Object.keys(billInstances)) {
      for (const inst of Object.values(billInstances[yyyymm])) {
        const tpl = bills.find(b => b.id === inst.billId);
        if (!tpl) continue;
        if (inst.expectedAmount != null && Math.abs(inst.expectedAmount - tpl.amount) > 0.01) {
          billsWithOverrides.push({
            name: tpl.name,
            month: yyyymm,
            template: tpl.amount,
            override: inst.expectedAmount,
          });
        }
      }
    }
    return {
      billsWithOverrides,
      categoriesWithYoYData: budgetCategories.filter(c => !c.archived).map(c => {
        const today = new Date();
        const startIso = `${today.getFullYear()}-${String(today.getMonth()+1).padStart(2,'0')}-01`;
        const endIso   = today.toISOString().slice(0, 10);
        const yoy = getCategoryYoYComparison(c.id, startIso, endIso);
        return { name: c.name, yoy };
      }).filter(x => x.yoy != null),
    };
  };
}


// ═══════════════════════════════════════════════════════════════════════════
//  BUDGET — Phase 5: Split-payment carry-over
//
//  Bills with a cycle longer than one month can opt into "split" payment,
//  where the user sets aside a portion each period (month or week) across
//  the cycle. This block:
//   - Renders a Carry-over tile on the dashboard with the household total.
//   - Tappable → opens a breakdown modal listing each split bill's progress.
//   - Re-runs whenever the dashboard re-renders.
// ═══════════════════════════════════════════════════════════════════════════

function _renderCarryOverTile() {
  const host = document.getElementById('budget-carryover-tile');
  if (!host) return;
  const { total, breakdown } = getTotalCarryOver();
  if (!breakdown.length) {
    host.style.display = 'none';
    return;
  }
  host.style.display = 'block';

  // Find the most pressing next-due bill among split bills (the one with
  // the smallest remaining funding gap, sorted by next-due date).
  let nextLabel = '';
  const upcoming = breakdown
    .filter(b => b.nextDueIso)
    .sort((a, b) => a.nextDueIso.localeCompare(b.nextDueIso));
  if (upcoming.length) {
    const next = upcoming[0];
    const remaining = Math.max(0, next.target - next.accrued);
    const dueLabel = _shortDate(next.nextDueIso);
    if (remaining > 0) {
      nextLabel = `Next: ${_escapeHtml(next.template.name)} · ${_money(remaining)} short by ${dueLabel}`;
    } else {
      nextLabel = `Next: ${_escapeHtml(next.template.name)} · fully funded · due ${dueLabel}`;
    }
  }

  host.innerHTML = `
    <div onclick="openCarryOverBreakdown()" style="cursor:pointer;background:linear-gradient(135deg,rgba(91,141,238,0.10),rgba(91,141,238,0.04));border:1px solid var(--border);border-radius:14px;padding:16px 18px;margin-bottom:14px">
      <div style="display:flex;justify-content:space-between;align-items:center;gap:12px">
        <div style="min-width:0">
          <div style="font-size:11px;font-weight:700;color:var(--muted);font-family:var(--mono);letter-spacing:0.5px;text-transform:uppercase;margin-bottom:4px">Carry-over set aside</div>
          <div style="font-size:24px;font-weight:700;font-family:var(--sans);color:var(--text);letter-spacing:-0.3px">${_money(total)}</div>
          ${nextLabel ? `<div style="font-size:11px;color:var(--muted);margin-top:4px">${nextLabel}</div>` : ''}
        </div>
        <div style="flex-shrink:0;display:flex;align-items:center;gap:6px;color:var(--muted);font-size:11px;font-family:var(--mono)">
          ${breakdown.length} bill${breakdown.length === 1 ? '' : 's'}
          <svg class="icon icon-sm" aria-hidden="true"><use href="#i-chevron-right"></use></svg>
        </div>
      </div>
    </div>`;
}

function openCarryOverBreakdown() {
  const { total, breakdown } = getTotalCarryOver();
  const host = document.getElementById('carryover-modal-list');
  const totalEl = document.getElementById('carryover-modal-total');
  if (totalEl) totalEl.textContent = _money(total);
  if (!host) return;

  if (!breakdown.length) {
    host.innerHTML = `<div style="padding:24px 16px;text-align:center;color:var(--muted);font-size:13px">No split-payment bills yet</div>`;
  } else {
    // Sort by next-due ascending; bills without a next-due date go last.
    breakdown.sort((a, b) => {
      if (!a.nextDueIso && !b.nextDueIso) return 0;
      if (!a.nextDueIso) return 1;
      if (!b.nextDueIso) return -1;
      return a.nextDueIso.localeCompare(b.nextDueIso);
    });
    host.innerHTML = breakdown.map(b => {
      const pct = Math.min(100, Math.round((b.accrued / b.target) * 100));
      const remaining = Math.max(0, b.target - b.accrued);
      const remainingLabel = remaining > 0
        ? `${_money(remaining)} to go`
        : 'Fully funded';
      const dueLabel = b.nextDueIso ? `due ${_shortDate(b.nextDueIso)}` : 'no upcoming due date';
      const unitLabel = b.unit === 'week' ? 'wk' : 'mo';
      return `
        <div style="padding:14px 16px;border-bottom:1px solid var(--border);cursor:pointer" onclick="closeModal('carryover-modal');openBillEditor('${b.template.id}')">
          <div style="display:flex;justify-content:space-between;align-items:baseline;gap:12px;margin-bottom:6px">
            <div style="font-size:14px;font-weight:600;color:var(--text);min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${_escapeHtml(b.template.name)}</div>
            <div style="font-family:var(--mono);font-size:13px;font-weight:700;color:var(--text);flex-shrink:0">${_money(b.accrued)} / ${_money(b.target)}</div>
          </div>
          <div style="height:6px;background:rgba(255,255,255,0.05);border-radius:3px;overflow:hidden;margin-bottom:6px">
            <div style="height:100%;width:${pct}%;background:var(--accent2);border-radius:3px"></div>
          </div>
          <div style="display:flex;justify-content:space-between;font-size:11px;color:var(--muted);font-family:var(--mono)">
            <span>${b.slot}/${b.totalSlots} ${unitLabel} · ${_money(b.perPeriod)}/${unitLabel}</span>
            <span>${remainingLabel} · ${dueLabel}</span>
          </div>
        </div>`;
    }).join('');
  }
  openModal('carryover-modal');
}

// ═══════════════════════════════════════════════════════════════════════════
//  MULTI-MONTH TIMELINE — opens a modal showing the full saving cycle for
//  one split-strategy bill. Each month in the cycle gets a row showing the
//  amount, state (saved/current/payment), and (for the current saving
//  month) Pay/Skip action buttons. Tapping the Edit button in the footer
//  opens the bill editor.
// ═══════════════════════════════════════════════════════════════════════════

// Currently-open timeline bill id (so action buttons inside the modal know
// which bill to act on without re-deriving from event payload).
let _multimonthTimelineBillId = null;

// Build the list of months in the bill's current cycle. Returns an array
// of { yyyymm, year, month, dueDate, instance, kind, label, status }
// objects, ordered chronologically. The cycle runs from the month AFTER
// the previous payment (or template creation, whichever is later) through
// to the next payment month inclusive.
function _buildMultiMonthTimelineRows(template) {
  if (!template || template.paymentStrategy !== 'split') return [];
  if (!template.splitInto || !template.splitInto.count) return [];

  const today = new Date();
  const todayYyyymm = _yyyymm(today);

  // Cycle anchor: previous theoretical payment (ignoring createdAt cap so
  // bills added mid-cycle still show the run from the actual cycle start).
  const prevDue    = _prevDueDateForTemplate(template, todayYyyymm, /* respectCreatedAt */ false);
  const lastPaid   = _lastPaidPaymentDueDate(template.id);
  const cycleStartIso = (lastPaid && (!prevDue || lastPaid > prevDue))
    ? lastPaid
    : (prevDue || (template.createdAt || _nowIso()).slice(0, 10));
  const cycleEndIso   = _nextDueDate(template.id) || _nextDueDateForTemplate(template, todayYyyymm);
  if (!cycleEndIso) return [];

  // Walk from the month containing the cycle start (or one after, depending
  // on whether the cycle start is a payment date) to the cycle-end month.
  // The first row we want is the month AFTER the prev payment — that's the
  // first saving month. The last row is the payment-end month.
  const startDate = new Date(cycleStartIso + 'T12:00:00');
  const endDate   = new Date(cycleEndIso + 'T12:00:00');
  // Begin at month following cycleStart UNLESS cycleStart is the start of
  // the cycle (createdAt with no prev payment in calendar). In practice
  // we always want the first saving month — which is the month after the
  // payment month.
  let cursorY = startDate.getFullYear();
  let cursorM = startDate.getMonth() + 1;
  if (cursorM > 11) { cursorM = 0; cursorY++; }
  const endY = endDate.getFullYear();
  const endM = endDate.getMonth();

  const perPeriod = Math.round((template.amount / template.splitInto.count) * 100) / 100;
  const rows = [];
  let safety = 0;
  while (safety++ < 36) {
    const inPaymentMonth = (cursorY === endY && cursorM === endM);
    const yyyymm = `${cursorY}-${String(cursorM + 1).padStart(2, '0')}`;
    const dom    = _clampDayOfMonth(template.dayOfMonth || 1, cursorY, cursorM);
    const dueDate = _isoDate(cursorY, cursorM, dom);
    const instance = _getInstance(yyyymm, template.id, dueDate);
    const monthLabel = new Date(cursorY, cursorM, 1).toLocaleDateString(undefined, { month: 'long', year: 'numeric' });

    const isCurrent = (yyyymm === todayYyyymm);
    let status, amount;
    if (inPaymentMonth) {
      status = instance?.paidAt ? 'paid' : 'payment';
      amount = template.amount; // full amount on the payment month
    } else if (instance?.skipped) {
      status = 'skipped';
      amount = perPeriod;
    } else if (instance?.paidAt) {
      status = 'saved';
      amount = perPeriod;
    } else {
      status = 'unpaid';  // current saving month (or future, before materialise)
      amount = perPeriod;
    }
    rows.push({
      yyyymm, year: cursorY, month: cursorM, dueDate, instance,
      isCurrent, inPaymentMonth, label: monthLabel, status, amount,
    });

    if (inPaymentMonth) break;
    cursorM++;
    if (cursorM > 11) { cursorM = 0; cursorY++; }
  }
  return rows;
}

// Open the timeline modal for a given bill id. Computes the cycle, renders
// rows, populates the status header.
function openMultiMonthTimeline(billId) {
  const tpl = bills.find(b => b.id === billId);
  if (!tpl) return;
  _multimonthTimelineBillId = billId;
  const co = getBillCarryOver(tpl);
  const rows = _buildMultiMonthTimelineRows(tpl);

  // Populate header
  const nameEl     = document.getElementById('mmtl-bill-name');
  const subEl      = document.getElementById('mmtl-subtitle');
  const statusEl   = document.getElementById('mmtl-status');
  const progAmtEl  = document.getElementById('mmtl-progress-amount');
  const progBarEl  = document.getElementById('mmtl-progress-bar');
  const metaEl     = document.getElementById('mmtl-meta');
  const listEl     = document.getElementById('mmtl-list');
  const editBtnEl  = document.getElementById('mmtl-edit-btn');
  if (nameEl) nameEl.textContent = tpl.name;
  if (subEl)  subEl.textContent  = `${_money(tpl.amount)} ${_frequencyLabel(tpl).toLowerCase()}`;

  if (co && statusEl) statusEl.textContent = `${co.slot} of ${co.totalSlots} months saved`;
  if (co && progAmtEl) progAmtEl.textContent = `${_money(co.accrued)} / ${_money(co.target)}`;
  if (co && progBarEl) {
    const pct = Math.min(100, Math.round((co.accrued / co.target) * 100));
    progBarEl.style.width = pct + '%';
  }
  if (co && metaEl) {
    const dueWhen = co.nextDueIso
      ? new Date(co.nextDueIso + 'T12:00:00').toLocaleDateString(undefined, { day: 'numeric', month: 'short', year: 'numeric' })
      : 'no upcoming payment';
    metaEl.innerHTML = `<span>${_money(co.perPeriod)}/mo</span><span>pays ${dueWhen}</span>`;
  }

  // Wire Edit button — close timeline first, then open editor
  if (editBtnEl) {
    editBtnEl.onclick = () => {
      closeModal('multimonth-timeline-modal');
      openBillEditor(billId);
    };
  }

  // Render rows
  if (listEl) {
    listEl.innerHTML = rows.map(r => _renderMultiMonthTimelineRow(r, tpl)).join('');
  }

  openModal('multimonth-timeline-modal');
}

// Render a single timeline row. `r` comes from _buildMultiMonthTimelineRows.
function _renderMultiMonthTimelineRow(r, tpl) {
  let statusBadge = '';
  let actions = '';
  let rowOpacity = '1';
  let amountColor = 'var(--text)';
  let icon = '';

  if (r.status === 'saved') {
    icon = `<svg style="width:14px;height:14px;color:var(--accent2)" aria-hidden="true"><use href="#i-check"></use></svg>`;
    statusBadge = `<span style="color:var(--accent2);font-size:11px;font-family:var(--mono);text-transform:uppercase;letter-spacing:0.5px">Saved</span>`;
    amountColor = 'var(--accent2)';
    rowOpacity = '0.85';
  } else if (r.status === 'skipped') {
    statusBadge = `<span style="color:var(--muted);font-size:11px;font-family:var(--mono);text-transform:uppercase;letter-spacing:0.5px">Skipped</span>`;
    rowOpacity = '0.55';
  } else if (r.status === 'unpaid' && r.isCurrent) {
    icon = `<svg style="width:14px;height:14px;color:var(--accent2)" aria-hidden="true"><use href="#i-piggy-bank"></use></svg>`;
    statusBadge = `<span style="color:var(--accent2);font-size:11px;font-family:var(--mono);text-transform:uppercase;letter-spacing:0.5px">This month</span>`;
    amountColor = 'var(--accent2)';
    actions = `
      <button class="bill-action-btn bill-action-paid" onclick="event.stopPropagation();handleMultiMonthTimelinePay('${r.yyyymm}','${r.dueDate}')" title="Set aside"><svg aria-hidden="true"><use href="#i-check"></use></svg></button>
      <button class="bill-action-btn bill-action-skip" onclick="event.stopPropagation();handleMultiMonthTimelineSkip('${r.yyyymm}','${r.dueDate}')" title="Skip"><svg aria-hidden="true"><use href="#i-x"></use></svg></button>`;
  } else if (r.status === 'unpaid') {
    statusBadge = `<span style="color:var(--muted);font-size:11px;font-family:var(--mono);text-transform:uppercase;letter-spacing:0.5px">Upcoming</span>`;
    rowOpacity = '0.7';
  } else if (r.status === 'payment') {
    icon = `<svg style="width:14px;height:14px;color:var(--accent)" aria-hidden="true"><use href="#i-banknote"></use></svg>`;
    statusBadge = `<span style="color:var(--accent);font-size:11px;font-family:var(--mono);text-transform:uppercase;letter-spacing:0.5px">Payment month</span>`;
    amountColor = 'var(--accent)';
    if (r.isCurrent && !r.instance?.paidAt) {
      actions = `
        <button class="bill-action-btn bill-action-paid" onclick="event.stopPropagation();handleMultiMonthTimelinePay('${r.yyyymm}','${r.dueDate}')" title="Mark paid"><svg aria-hidden="true"><use href="#i-check"></use></svg></button>`;
    }
  } else if (r.status === 'paid') {
    icon = `<svg style="width:14px;height:14px;color:var(--accent2)" aria-hidden="true"><use href="#i-check"></use></svg>`;
    statusBadge = `<span style="color:var(--accent2);font-size:11px;font-family:var(--mono);text-transform:uppercase;letter-spacing:0.5px">Paid</span>`;
    amountColor = 'var(--accent2)';
    rowOpacity = '0.85';
  }

  return `
    <div style="display:flex;align-items:center;gap:12px;padding:14px 16px;border-bottom:1px solid var(--border);opacity:${rowOpacity}">
      <div style="width:24px;flex-shrink:0;display:flex;align-items:center;justify-content:center">${icon}</div>
      <div style="flex:1;min-width:0">
        <div style="font-size:13px;color:var(--text);font-weight:500">${_escapeHtml(r.label)}</div>
        <div style="margin-top:3px">${statusBadge}</div>
      </div>
      <div style="font-family:var(--mono);font-size:14px;font-weight:700;color:${amountColor};flex-shrink:0">${_money(r.amount)}</div>
      <div style="display:flex;gap:4px;flex-shrink:0">${actions}</div>
    </div>`;
}

// Pay/skip action handlers for timeline rows. They mark the instance,
// then re-render the timeline AND the budget so both views stay in sync.
async function handleMultiMonthTimelinePay(yyyymm, dueDate) {
  if (!_multimonthTimelineBillId) return;
  const billId = _multimonthTimelineBillId;
  const tpl = bills.find(b => b.id === billId);
  if (!tpl) return;
  // Make sure the month is materialised (so the saving instance exists to
  // be marked paid). Idempotent.
  if (typeof materialiseMonth === 'function') {
    await materialiseMonth(yyyymm, { persist: true });
  }
  await markBillPaid(yyyymm, billId, { dueDate });
  // Refresh both views
  openMultiMonthTimeline(billId);
  if (typeof renderBudget === 'function') await renderBudget();
  toast(`${_money((tpl.amount / (tpl.splitInto?.count || 1)))} set aside for ${tpl.name}`);
}

async function handleMultiMonthTimelineSkip(yyyymm, dueDate) {
  if (!_multimonthTimelineBillId) return;
  const billId = _multimonthTimelineBillId;
  if (typeof materialiseMonth === 'function') {
    await materialiseMonth(yyyymm, { persist: true });
  }
  await skipBillInstance(yyyymm, billId, dueDate);
  openMultiMonthTimeline(billId);
  if (typeof renderBudget === 'function') await renderBudget();
}

// Wrap the existing dashboard render once more so the carry-over tile is
// updated on every dashboard refresh. Preserves the Phase 4b override.
const _phase5RenderBudgetDashboard = renderBudgetDashboard;
renderBudgetDashboard = function() {
  _phase5RenderBudgetDashboard.call(this);
  _renderCarryOverTile();
  _renderSafeToSpendTile();
};

// ═══════════════════════════════════════════════════════════════════════════
//  BUDGET — Phase 5b: "Safe to spend" tile
//
//  Headline number = how much is genuinely free after committing every
//  pound that already has a job. Intentionally conservative.
//
//    Safe to spend (current/future month) =
//        end-of-month projected balance       (incl. income still to come, bills still due)
//      − unspent budget remaining             (what you've earmarked but not yet spent)
//      − total carry-over                     (set aside for future split bills)
//
//    Safe to spend (past month) = actual surplus that month
//      = income received − bills paid − discretionary spend
//
//  We use projectCashFlow (the same engine driving the cash-flow chart) so
//  the number stays consistent with what's shown there.
// ═══════════════════════════════════════════════════════════════════════════

// Sum of the absolute "remaining budget" across all active categories, in
// the given calendar month. Categories without a budget contribute 0. Over-
// budget categories contribute negatively (so they pull the safe-to-spend
// number down — you've borrowed against future). Returns rounded GBP number.
function _getUnspentBudgetForMonth(yyyymm) {
  let total = 0;
  for (const cat of getActiveBudgetCategories()) {
    let monthlyBudget;
    if (cat.budgetCycle === 'monthly') monthlyBudget = cat.monthlyBudget;
    else if (cat.weeklyBudget != null) monthlyBudget = cat.weeklyBudget * 4.345;
    else monthlyBudget = null;
    if (monthlyBudget == null) continue; // no budget set — don't subtract anything
    const spent = getSpendForCategory(yyyymm, cat.id);
    total += (monthlyBudget - spent);
  }
  return Math.round(total * 100) / 100;
}

// Sum of income entries marked `paidAt` for the given month.
function _getReceivedIncomeForMonth(yyyymm) {
  let total = 0;
  for (const e of getIncomeEntriesForMonth(yyyymm)) {
    if (!e.paidAt) continue;
    total += (e.actualAmount ?? e.amount) || 0;
  }
  return Math.round(total * 100) / 100;
}

// Sum of expected income entries (paidAt === null) for the given month,
// excluding phantom entries from deleted/archived templates. Used in the
// Safe-to-spend breakdown so the "income still to come" line is honest.
function _getExpectedIncomeForMonth(yyyymm) {
  const primary = getPrimaryAccount();
  if (!primary) return 0;
  let total = 0;
  const seenTemplateDates = new Set();
  for (const e of getIncomeEntriesForMonth(yyyymm)) {
    if (e.accountId !== primary.id) continue;
    // Register this materialised instance so the projection loop below
    // never re-adds it as "still to come" — this MUST happen for received
    // entries too, otherwise marking income as received causes the unpaid
    // template instance for the same date to leak back in (the bug this
    // function exists to prevent). Mirrors _eventsOnDay's ordering, where
    // the template is recorded before the paidAt skip.
    if (e.templateId) {
      const tpl = getIncomeTemplateById(e.templateId);
      if (!tpl || tpl.archived) continue;          // phantom — skip entirely
      seenTemplateDates.add(`${e.templateId}__${e.date}`);
    }
    if (e.paidAt) continue;                         // already received — not still-to-come
    total += (e.actualAmount ?? e.amount) || 0;
  }
  // Also include unmaterialised template instances landing in this month.
  const { year, month } = _parseYyyymm(yyyymm);
  for (const tpl of (incomeTemplates || [])) {
    if (tpl.archived) continue;
    if (tpl.accountId && tpl.accountId !== primary.id) continue;
    if (!tpl.accountId && !primary.isPrimary) continue;
    const dates = (typeof getInstanceDatesInMonth === 'function')
      ? getInstanceDatesInMonth(tpl, year, month)
      : [];
    for (const d of dates) {
      const key = `${tpl.id}__${d}`;
      if (seenTemplateDates.has(key)) continue;
      const amt = (typeof getProjectedIncomeAmount === 'function')
        ? getProjectedIncomeAmount(tpl)
        : tpl.amount;
      total += amt || 0;
    }
  }
  return Math.round(total * 100) / 100;
}

// Returns { startBalance, endBalance } for the target month using
// projectCashFlow. For the current month, startBalance = balance now.
// For future months, we walk forward through every prior month so all
// scheduled bills and income are applied.
function _getMonthEndProjection(yyyymm) {
  const todayIso = (new Date()).toISOString().slice(0, 10);
  const { year, month } = _parseYyyymm(yyyymm);
  const lastDay = _daysInMonth(year, month);
  const endIso  = _isoDate(year, month, lastDay);
  const a = new Date(todayIso + 'T12:00:00');
  const b = new Date(endIso   + 'T12:00:00');
  const daysAhead = Math.max(1, Math.ceil((b.getTime() - a.getTime()) / MS_PER_DAY) + 1);
  const proj = projectCashFlow(null, daysAhead);
  if (!proj.setupComplete) return null;
  const endPoint = proj.points.find(p => p.date === endIso);
  const endBalance = endPoint ? endPoint.balance
                              : (proj.points[proj.points.length - 1]?.balance ?? proj.startBalance);
  return { startBalance: proj.startBalance, endBalance };
}

// Returns { amount, mode, breakdown } where mode ∈ 'past' | 'current' |
// 'future' | 'no-setup'. The breakdown carries enough sub-totals for the
// modal to render every line as its own row.
function getSafeToSpend(yyyymm) {
  const todayMonth = _yyyymm(new Date());
  const isFuture = yyyymm > todayMonth;
  const isPast   = yyyymm < todayMonth;

  if (isPast) {
    // Past months: actual P&L for the month — what was left after the dust
    // settled. Carry-over is current-state, so we don't subtract it here.
    const incomeReceived = _getReceivedIncomeForMonth(yyyymm);
    const billsPaid      = getPaidSoFar(yyyymm);
    const spend          = getTotalSpendForMonth(yyyymm);
    return {
      amount: Math.round((incomeReceived - billsPaid - spend) * 100) / 100,
      mode: 'past',
      breakdown: { incomeReceived, billsPaid, spend },
    };
  }

  const proj = _getMonthEndProjection(yyyymm);
  if (proj == null) {
    return { amount: null, mode: 'no-setup', breakdown: null };
  }

  // For the *current* month we want a transparent breakdown:
  //   balance now + income still to come − bills still to pay
  //                = balance at month end
  // For *future* months, "balance now" + "income to come" + "bills to pay"
  // span multiple months and aren't directly meaningful, so we use the
  // projected balance at month-end as the starting point and just show
  // budget + carry-over deductions.
  const budgetRemaining = _getUnspentBudgetForMonth(yyyymm);
  const carryOver       = getTotalCarryOver().total;

  if (isFuture) {
    return {
      amount: Math.round((proj.endBalance - budgetRemaining - carryOver) * 100) / 100,
      mode: 'future',
      breakdown: {
        endBalance: proj.endBalance,
        budgetRemaining,
        carryOver,
      },
    };
  }

  // Current month — break it apart for the modal
  const balanceNow   = proj.startBalance;
  const incomeToCome = _getExpectedIncomeForMonth(yyyymm);
  const billsToPay   = getLeftToPay(yyyymm);

  return {
    amount: Math.round((balanceNow + incomeToCome - billsToPay - budgetRemaining - carryOver) * 100) / 100,
    mode: 'current',
    breakdown: {
      balanceNow,
      incomeToCome,
      billsToPay,
      budgetRemaining,
      carryOver,
    },
  };
}

function _renderSafeToSpendTile() {
  const host = document.getElementById('budget-safe-to-spend-tile');
  if (!host) return;
  const yyyymm = _budgetViewMonth || _yyyymm(new Date());
  const result = getSafeToSpend(yyyymm);

  if (result.mode === 'no-setup') {
    // No primary account / balance set — hide the tile rather than show a
    // misleading zero. The hero card already nudges users to set up an
    // account elsewhere.
    host.style.display = 'none';
    return;
  }
  host.style.display = 'block';

  const amt = result.amount;
  const isNegative = amt < 0;
  const colorClass = isNegative ? 'var(--danger)' : 'var(--ok)';
  const subLabel = result.mode === 'past'    ? 'After bills, income & spend that month'
                 : result.mode === 'future'  ? 'Projected after bills, budgets & carry-over'
                 :                              'After bills, budgets & carry-over';

  host.innerHTML = `
    <div onclick="openSafeToSpendBreakdown()" style="cursor:pointer;background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:16px 18px;margin-bottom:14px">
      <div style="display:flex;justify-content:space-between;align-items:center;gap:12px">
        <div style="min-width:0;flex:1">
          <div style="font-size:11px;font-weight:700;color:var(--muted);font-family:var(--mono);letter-spacing:0.5px;text-transform:uppercase;margin-bottom:4px">Safe to spend</div>
          <div style="font-size:28px;font-weight:700;font-family:var(--sans);color:${colorClass};letter-spacing:-0.5px">${_money(amt)}</div>
          <div style="font-size:11px;color:var(--muted);margin-top:4px">${subLabel}</div>
        </div>
        <div style="flex-shrink:0;color:var(--muted)">
          <svg class="icon icon-sm" aria-hidden="true"><use href="#i-chevron-right"></use></svg>
        </div>
      </div>
    </div>`;
}

function openSafeToSpendBreakdown() {
  const yyyymm = _budgetViewMonth || _yyyymm(new Date());
  const result = getSafeToSpend(yyyymm);
  const titleEl = document.getElementById('safe-modal-month');
  const amountEl = document.getElementById('safe-modal-amount');
  const bodyEl = document.getElementById('safe-modal-body');
  if (!titleEl || !amountEl || !bodyEl) return;

  const { year, month } = _parseYyyymm(yyyymm);
  const monthLabel = new Date(year, month, 1).toLocaleDateString(undefined, { month: 'long', year: 'numeric' });
  titleEl.textContent = monthLabel;
  amountEl.textContent = result.amount != null ? _money(result.amount) : '—';
  amountEl.style.color = (result.amount != null && result.amount < 0) ? 'var(--danger)' : 'var(--ok)';

  if (result.mode === 'no-setup') {
    bodyEl.innerHTML = `<div style="padding:20px;text-align:center;color:var(--muted);font-size:13px">Set a primary account with a current balance to see this calculation. Go to Accounts to add one.</div>`;
    openModal('safe-to-spend-modal');
    return;
  }

  // Render a row. `linkAction` (optional) becomes the onclick, and the row
  // gets a hover affordance when present. Sub-totals (subtle) are dimmer.
  const row = (label, value, sign, opts = {}) => {
    const subtle = !!opts.subtle;
    const linkAction = opts.linkAction || null;
    const valueColor = subtle ? 'var(--muted)' : 'var(--text)';
    const cursor = linkAction ? 'cursor:pointer' : '';
    const onclick = linkAction ? `onclick="${linkAction}"` : '';
    const hover = linkAction ? 'class="safe-modal-row-link"' : '';
    const arrow = linkAction
      ? `<svg class="icon icon-sm" aria-hidden="true" style="color:var(--muted);margin-left:6px;flex-shrink:0"><use href="#i-chevron-right"></use></svg>`
      : '';
    const labelCol = `<div style="display:flex;align-items:center;font-size:13px;color:${subtle ? 'var(--muted)' : 'var(--text)'};min-width:0">
        <span style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${label}</span>${arrow}
      </div>`;
    return `
      <div ${hover} ${onclick} style="display:flex;justify-content:space-between;align-items:baseline;padding:10px 16px;border-bottom:1px solid var(--border);${cursor}">
        ${labelCol}
        <div style="font-family:var(--mono);font-size:14px;font-weight:${subtle ? '500' : '600'};color:${valueColor};flex-shrink:0;margin-left:12px">${sign}${_money(Math.abs(value))}</div>
      </div>`;
  };

  // Final totals row (bold, highlighted)
  const totalRow = (label, value) => {
    const color = value < 0 ? 'var(--danger)' : 'var(--ok)';
    return `
      <div style="display:flex;justify-content:space-between;align-items:baseline;padding:14px 16px;background:rgba(255,255,255,0.02)">
        <div style="font-size:13px;font-weight:700">${label}</div>
        <div style="font-family:var(--mono);font-size:15px;font-weight:700;color:${color}">${_money(value)}</div>
      </div>`;
  };

  // Navigation helpers — close the modal then switch panel
  const goBills      = `closeModal('safe-to-spend-modal');budgetSwitchPanel('bills')`;
  const goSpend      = `closeModal('safe-to-spend-modal');budgetSwitchPanel('spend')`;
  const goCarryOver  = `closeModal('safe-to-spend-modal');setTimeout(openCarryOverBreakdown,80)`;

  if (result.mode === 'past') {
    const { incomeReceived, billsPaid, spend } = result.breakdown;
    bodyEl.innerHTML = `
      ${row('Income received',     incomeReceived, '+')}
      ${row('Bills paid',          billsPaid,      '−', { linkAction: goBills })}
      ${row('Discretionary spend', spend,          '−', { linkAction: goSpend })}
      ${totalRow('Surplus / shortfall', result.amount)}
      <div style="padding:12px 16px;font-size:11px;color:var(--muted);border-top:1px solid var(--border)">Past month: shows what was actually left over once income, bills and spend had all settled.</div>`;
  } else if (result.mode === 'future') {
    const { endBalance, budgetRemaining, carryOver } = result.breakdown;
    bodyEl.innerHTML = `
      <div style="padding:10px 16px;font-size:11px;color:var(--accent);background:rgba(232,168,56,0.06);border-bottom:1px solid var(--border)">Projected — assumes bills, income and budgets continue as set up.</div>
      ${row('Projected balance at month end', endBalance, '')}
      ${row('Less: budget still to spend', budgetRemaining, '−', { subtle: true, linkAction: goSpend })}
      ${row('Less: carry-over set aside',  carryOver,       '−', { subtle: true, linkAction: goCarryOver })}
      ${totalRow('Safe to spend', result.amount)}
      <div style="padding:12px 16px;font-size:11px;color:var(--muted);border-top:1px solid var(--border)">If you stick to your category budgets and pay all bills, this is what's truly free. Negative means you'd need to cut something or top up the account.</div>`;
  } else {
    // Current month — fully transparent breakdown
    const { balanceNow, incomeToCome, billsToPay, budgetRemaining, carryOver } = result.breakdown;
    bodyEl.innerHTML = `
      ${row('Account balance now',          balanceNow,      '')}
      ${row('Plus: income still to come',   incomeToCome,    '+', { subtle: true })}
      ${row('Less: bills still to pay',     billsToPay,      '−', { subtle: true, linkAction: goBills })}
      ${row('Less: budget still to spend',  budgetRemaining, '−', { subtle: true, linkAction: goSpend })}
      ${row('Less: carry-over set aside',   carryOver,       '−', { subtle: true, linkAction: goCarryOver })}
      ${totalRow('Safe to spend', result.amount)}
      <div style="padding:12px 16px;font-size:11px;color:var(--muted);border-top:1px solid var(--border)">If you stick to your category budgets and pay all bills, this is what's truly free. Tap a row to jump to the relevant section.</div>`;
  }

  openModal('safe-to-spend-modal');
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
  const gaps = dts.slice(1).map((t,i) => (t-dts[i])/MS_PER_DAY);
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
          <span style="text-align:right;font-family:var(--mono);font-size:11px;color:var(--ok)">${_money(r.price)}</span>
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
    const next=new Date(last.getTime()+g.avgInterval*MS_PER_DAY);
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
          <span style="font-size:11px;color:var(--ok);font-weight:600">${_money(g.totalSpend)} spent</span>
          ${g.avgPrice?`<span style="font-size:11px;color:var(--muted)">avg ${_money(g.avgPrice)}</span>`:''}
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
          <div style="font-size:9px;color:var(--ok);font-family:var(--mono)">${currencySymbol()}${item.price.toFixed(0)}</div>
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
              <div style="font-size:10px;color:var(--muted);font-family:var(--mono);margin-top:1px">${esc(asin)} · ${asinItems.length} orders · avg ${_money(asinAvg)}</div>
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
              <div style="font-size:11px;color:var(--muted);margin-top:1px">${it.logs?.length||0} log entries${it.store?` · ${esc(it.store)}`:''}${avgP?` · avg ${_money(avgP)}`:''}</div>
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
        <div style="font-size:11px;color:var(--muted);text-align:right;flex-shrink:0"><div>${m.group.items.length} orders</div><div style="color:var(--ok)">${_money(m.group.totalSpend)}</div></div>
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
        <div style="font-size:11px;color:var(--muted);margin-top:2px">${esc(m.group.category)} · ${m.group.items.length} orders · ${_azIntervalLabel(m.group.avgInterval)} · <span style="color:var(--ok)">${_money(m.group.totalSpend)}</span></div>
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

// ═══════════════════════════════════════════════════════════════════════════
//  BUDGET — Basic Mode
//
//  A read-only month timeline. Vertical flow:
//    Account balance (start of month, or current for current month)
//    ↓
//    Income events by date (actuals where present, expected otherwise)
//    Bills by date (lump-sum + payment-month split bills)
//    Carried forward (this month's saving set-asides)
//    Spend left (unspent budget)
//    ↓
//    Safe to spend
//
//  No interactivity — every figure mirrors what you'd see if you stepped
//  through Dashboard / Bills / Spend / Accounts tabs and added it up.
// ═══════════════════════════════════════════════════════════════════════════

function _basicSectionLabel(text) {
  return `<div style="font-size:10px;font-weight:700;color:var(--muted);font-family:var(--mono);letter-spacing:1px;text-transform:uppercase;margin:14px 0 8px 0">${text}</div>`;
}

// One row in the timeline. Date label on the left (or section label),
// amount on the right. `direction` is 'in' (positive, accent2) or 'out'
// (negative, danger). `subtle` for less important rows (set aside, etc.)
function _basicTimelineRow(dateLabel, label, amount, direction, opts = {}) {
  const subtle = opts.subtle === true;
  const valueColor = direction === 'in'
    ? 'var(--accent2)'
    : (subtle ? 'var(--muted)' : 'var(--text)');
  const sign = direction === 'in' ? '+' : '−';
  const rowOpacity = subtle ? '0.85' : '1';
  return `
    <div style="display:flex;align-items:center;gap:14px;padding:10px 0;border-bottom:1px solid var(--border);opacity:${rowOpacity}">
      <div style="font-family:var(--mono);font-size:11px;color:var(--muted);width:60px;flex-shrink:0">${dateLabel || ''}</div>
      <div style="flex:1;min-width:0;font-size:13px;color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${label}</div>
      <div style="font-family:var(--mono);font-size:14px;font-weight:600;color:${valueColor};flex-shrink:0">${sign}${_money(Math.abs(amount))}</div>
    </div>`;
}

// Header / footer "balance" row — bigger, bolder.
function _basicBalanceRow(label, amount, opts = {}) {
  const isResult = opts.result === true;
  const color = isResult
    ? (amount < 0 ? 'var(--danger)' : 'var(--ok)')
    : 'var(--text)';
  const bg = isResult
    ? 'background:linear-gradient(135deg,rgba(80,200,140,0.10),rgba(80,200,140,0.04));'
    : 'background:linear-gradient(135deg,rgba(91,141,238,0.10),rgba(91,141,238,0.04));';
  return `
    <div style="${bg}border:1px solid var(--border);border-radius:10px;padding:14px 16px;display:flex;justify-content:space-between;align-items:baseline;gap:12px">
      <div style="font-size:13px;font-weight:700;color:var(--text)">${label}</div>
      <div style="font-family:var(--mono);font-size:20px;font-weight:700;color:${color}">${_money(amount)}</div>
    </div>`;
}

function renderBudgetBasicMode() {
  const host = document.getElementById('budget-basic-content');
  if (!host) return;

  const yyyymm = _budgetViewMonth || _yyyymm(new Date());
  const todayMonth = _yyyymm(new Date());
  const isPast    = yyyymm < todayMonth;
  const isFuture  = yyyymm > todayMonth;
  const todayIso  = (new Date()).toISOString().slice(0, 10);
  const { year, month } = _parseYyyymm(yyyymm);
  const monthLabel = new Date(year, month, 1).toLocaleDateString(undefined, { month: 'long', year: 'numeric' });

  const safe = getSafeToSpend(yyyymm);
  if (!safe || safe.mode === 'no-setup') {
    host.innerHTML = `
      <div style="padding:60px 20px;text-align:center;color:var(--muted)">
        <div style="font-size:14px;margin-bottom:8px;color:var(--text);font-weight:600">Basic Mode unavailable</div>
        <div style="font-size:13px">Set up a primary account to see your monthly timeline here.</div>
      </div>`;
    return;
  }

  // ── 1. Starting balance ──────────────────────────────────────────────────
  // For current month: balance now (the dashboard's "Account balance now").
  // For past: implied month-start balance (incomeReceived - billsPaid - spend
  //   = end balance, so start = end + spend + bills - income… no, simpler:
  //   we use the month's actual P&L instead of a balance line).
  // For future: projected month-end balance from the cash-flow model.
  let startBalance = null;
  let startLabel   = '';
  if (isPast) {
    // Past month — show the actuals instead of starting balance up top
    // (Pete's spec assumes "now" view; for past months we'll do a simpler
    // P&L breakdown). We still draw a balance-ish header for symmetry.
    const incomeReceived = safe.breakdown.incomeReceived || 0;
    const billsPaid      = safe.breakdown.billsPaid      || 0;
    const spend          = safe.breakdown.spend          || 0;
    host.innerHTML = `
      <div style="max-width:560px;margin:0 auto">
        <div style="margin-bottom:6px;font-size:11px;color:var(--muted);font-family:var(--mono);letter-spacing:1px;text-transform:uppercase">${monthLabel} — past month</div>
        ${_basicBalanceRow('Income received', incomeReceived)}
        ${_basicSectionLabel('Outflows')}
        <div>
          ${_basicTimelineRow('', 'Bills paid this month', -billsPaid, 'out')}
          ${_basicTimelineRow('', 'Discretionary spend',   -spend,     'out')}
        </div>
        <div style="margin-top:14px">${_basicBalanceRow(safe.amount >= 0 ? 'Left over' : 'Overspent', safe.amount, { result: true })}</div>
        <div style="font-size:11px;color:var(--muted);margin-top:10px;line-height:1.5">A simple summary of how the month landed. ${safe.amount >= 0 ? 'You stayed within your means.' : 'Outflows exceeded income.'}</div>
      </div>`;
    return;
  }

  if (isFuture) {
    startBalance = safe.breakdown.endBalance;
    startLabel   = 'Projected end-of-month balance';
  } else {
    startBalance = safe.breakdown.balanceNow;
    startLabel   = 'Account balance now';
  }

  // ── 2. Gather timeline events ────────────────────────────────────────────
  // Each event: { dateIso, label, amount, direction, dateLabel }

  const events = [];

  // Income events. The balance-at-top already reflects:
  //   - For current month: actuals received so far (they're in the user's
  //     bank balance). We must NOT add them again — only unpaid future
  //     expected income gets a row.
  //   - For future month: the balance-at-top is the projected end-of-month
  //     balance, which already accounts for all of next month's income.
  //     We could show events for transparency, but they'd already be in
  //     the projected balance — confusing. Treat future like current and
  //     show only the unpaid expected events (they're the projection's
  //     basis), but don't add them above the balance.
  // For SIMPLICITY and to keep the math sane, current month shows only
  // unpaid future-dated income; future months show all expected income
  // as informational (without re-adding to the running total).
  const incomeEntriesThisMonth = (typeof getIncomeEntriesForMonth === 'function')
    ? getIncomeEntriesForMonth(yyyymm)
    : [];
  // Keys for already-handled income (date|label) so the unmaterialised
  // template loop below never re-adds an instance we've already accounted
  // for. This MUST include received income too — otherwise marking income
  // as received drops it from `events` here but lets the template loop
  // re-add it as upcoming (same bug class as the Safe-to-spend tile). The
  // receipt is already reflected in balanceNow.
  const seenIncomeKeys = new Set();
  for (const e of incomeEntriesThisMonth) {
    if (e.skipped) continue;
    const tpl = e.templateId ? (typeof getIncomeTemplateById === 'function' ? getIncomeTemplateById(e.templateId) : null) : null;
    if (e.templateId && (!tpl || tpl.archived)) continue; // phantom — skip
    const label = tpl?.name || e.notes || 'Income';
    const dateIso = e.date;
    const amount = (e.actualAmount ?? e.amount) || 0;
    if (amount <= 0) continue;
    // Mark this date|label as handled regardless of paid state, so the
    // template loop won't duplicate it.
    seenIncomeKeys.add(dateIso + '|' + label);
    // Skip already-received income (it's in balanceNow already, can't be
    // added again). Skip past-dated unpaid income for the current month
    // (didn't arrive — would be confusing to project).
    if (e.paidAt) continue;
    if (!isFuture && dateIso < todayIso) continue;
    events.push({
      dateIso, label, amount, direction: 'in',
      dateLabel: _basicShortDate(dateIso),
      kind: 'income',
    });
  }
  // Unmaterialised template income for the rest of the month
  for (const tpl of (incomeTemplates || [])) {
    if (tpl.archived) continue;
    const dates = (typeof getInstanceDatesInMonth === 'function')
      ? getInstanceDatesInMonth(tpl, year, month)
      : [];
    for (const d of dates) {
      const k = d + '|' + tpl.name;
      if (seenIncomeKeys.has(k)) continue;
      if (!isFuture && d < todayIso) continue;  // past-dated, never received
      events.push({
        dateIso: d, label: tpl.name, amount: tpl.amount || 0,
        direction: 'in', dateLabel: _basicShortDate(d), kind: 'income',
      });
    }
  }

  // Bill events — payment instances (lump + payment-month split). Saving
  // instances are aggregated separately as "carried forward".
  const billInsts = Object.values(getMonthInstances(yyyymm) || {});
  for (const inst of billInsts) {
    if (inst.skipped) continue;
    if (inst.kind === 'saving') continue; // handled by carry-over line
    if (inst.paidAt) continue; // already left the account, in balanceNow
    const tpl = bills.find(b => b.id === inst.billId);
    if (!tpl) continue;
    const dateIso = inst.dueDate;
    const amount  = (inst.actualAmount ?? inst.expectedAmount) || 0;
    if (amount <= 0) continue;
    events.push({
      dateIso, label: tpl.name, amount,
      direction: 'out',
      dateLabel: _basicShortDate(dateIso),
      kind: 'bill',
    });
  }
  // Bill templates that should land on a date but aren't materialised yet
  const materialisedKeys = new Set(billInsts.map(i => i.billId + '|' + i.dueDate));
  for (const tpl of (bills || [])) {
    if (tpl.archived) continue;
    if (tpl.paymentStrategy === 'split') continue; // saving rows handled separately
    const dates = (typeof getInstanceDatesInMonth === 'function')
      ? getInstanceDatesInMonth(tpl, year, month)
      : [];
    for (const d of dates) {
      const k = tpl.id + '|' + d;
      if (materialisedKeys.has(k)) continue;
      events.push({
        dateIso: d, label: tpl.name, amount: tpl.amount || 0,
        direction: 'out', dateLabel: _basicShortDate(d), kind: 'bill',
      });
    }
  }

  // Sort all dated events chronologically.
  events.sort((a, b) => a.dateIso.localeCompare(b.dateIso));

  // ── 3. Aggregated end-of-month deductions ────────────────────────────────
  const carryOver = safe.breakdown.carryOver || 0;
  const budget    = safe.breakdown.budgetRemaining || 0;
  const carryCount = (typeof getTotalCarryOver === 'function')
    ? (getTotalCarryOver().breakdown.length || 0)
    : 0;

  // Income / bill events split by direction (so we can render them in
  // separate sections for clarity, but still sorted within each).
  const incomeEvents = events.filter(e => e.direction === 'in');
  const billEvents   = events.filter(e => e.direction === 'out');

  // ── 4. Render ────────────────────────────────────────────────────────────
  let html = `
    <div style="max-width:560px;margin:0 auto">
      <div style="margin-bottom:10px;font-size:11px;color:var(--muted);font-family:var(--mono);letter-spacing:1px;text-transform:uppercase">${monthLabel}${isFuture ? ' — projected' : ''}</div>
      ${_basicBalanceRow(startLabel, startBalance)}`;

  // Subtotal row helper — shown beneath each event group so the user can
  // see at a glance what the section adds up to without scanning + adding
  // the dated rows themselves.
  const subtotalRow = (label, total, direction) => {
    const sign = direction === 'in' ? '+' : '−';
    const color = direction === 'in' ? 'var(--accent2)' : 'var(--text)';
    return `
      <div style="display:flex;align-items:center;gap:14px;padding:10px 0;border-top:1px solid var(--border);margin-top:2px">
        <div style="width:60px;flex-shrink:0"></div>
        <div style="flex:1;min-width:0;font-size:11px;font-family:var(--mono);text-transform:uppercase;letter-spacing:0.5px;color:var(--muted);font-weight:700">${label}</div>
        <div style="font-family:var(--mono);font-size:14px;font-weight:700;color:${color};flex-shrink:0">${sign}${_money(Math.abs(total))}</div>
      </div>`;
  };

  if (incomeEvents.length) {
    const incomeTotal = incomeEvents.reduce((s, e) => s + e.amount, 0);
    html += _basicSectionLabel('Money coming in');
    html += '<div>' + incomeEvents.map(e => _basicTimelineRow(e.dateLabel, _escapeHtml(e.label), e.amount, 'in')).join('') + subtotalRow('Total coming in', incomeTotal, 'in') + '</div>';
  }

  if (billEvents.length) {
    const billsTotal = billEvents.reduce((s, e) => s + e.amount, 0);
    html += _basicSectionLabel('Bills');
    html += '<div>' + billEvents.map(e => _basicTimelineRow(e.dateLabel, _escapeHtml(e.label), -e.amount, 'out')).join('') + subtotalRow('Total bills', billsTotal, 'out') + '</div>';
  }

  if (carryOver > 0) {
    html += _basicSectionLabel('Set aside');
    html += '<div>' + _basicTimelineRow('', `Carrying forward${carryCount ? ` (${carryCount} bill${carryCount === 1 ? '' : 's'})` : ''}`, -carryOver, 'out', { subtle: true }) + '</div>';
  }

  if (budget > 0) {
    html += _basicSectionLabel('Discretionary');
    html += '<div>' + _basicTimelineRow('', 'Spend left (budgets)', -budget, 'out', { subtle: true }) + '</div>';
  }

  html += `<div style="margin-top:18px">${_basicBalanceRow('Safe to spend', safe.amount, { result: true })}</div>`;

  // Sub-text caption to set expectations
  html += `<div style="font-size:11px;color:var(--muted);margin-top:12px;line-height:1.5">A read-only summary of where ${isFuture ? 'next month' : 'this month'} is heading. Tap Dashboard, Bills, or Spend above to make changes.</div>`;
  html += '</div>';

  host.innerHTML = html;
}

// Short date for the basic-mode timeline rows: "5 May".
function _basicShortDate(iso) {
  if (!iso) return '';
  const d = new Date(iso + 'T12:00:00');
  return d.toLocaleDateString(undefined, { day: 'numeric', month: 'short' });
}

// Re-render basic mode whenever the budget view changes (month switcher,
// data updates after navigation, etc.). Hook into renderBudget so it
// stays in sync when the user is viewing the basic panel.
const _phaseBasicRenderBudget = renderBudget;
renderBudget = async function() {
  await _phaseBasicRenderBudget.call(this);
  if (_budgetActivePanel === 'basic' && typeof renderBudgetBasicMode === 'function') {
    renderBudgetBasicMode();
  }
};
