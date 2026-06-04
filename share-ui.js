// ============================================================================
// share-ui.js  —  Household Share Access management UI (lazy-loaded)
// ----------------------------------------------------------------------------
// Extracted from app.js. Loaded on demand via window._loadShareUI() — only
// when the user opens Settings (Share Access section) or taps Share in the
// bulk-select bar. The shared-DATA sync engine, share crypto, record
// filtering, card-action share buttons, and the join/auth gate all REMAIN in
// app.js (they run on boot/sync). loadShareTargets() and updateHouseholdShareUI()
// also stay core; this file only holds the render/modal/management surface.
// All functions are declared as plain globals (no module wrapper) so existing
// onclick= handlers and core call sites resolve exactly as before.
// ============================================================================

function bulkShare() {
  const spec = _getActiveSpec();
  const set  = _getActiveSelection();
  if (!spec || !set || !set.size) return;
  if (!isOwner()) { toast('Only the owner can share'); return; }
  const n = set.size;
  const noun = (n === 1) ? spec.noun : spec.pluralNoun;
  const existing = Array.isArray(_shareTargets) ? _shareTargets.filter(t => !t._deletedAt) : [];

  document.getElementById('bulk-share-overlay')?.remove();
  const overlay = document.createElement('div');
  overlay.id = 'bulk-share-overlay';
  overlay.style.cssText = 'position:fixed;inset:0;z-index:650;background:rgba(0,0,0,0.7);display:flex;align-items:center;justify-content:center;padding:20px;backdrop-filter:blur(4px)';
  _hideFabForCustomOverlay(overlay);

  const existingRows = existing.length ? existing.map(t => `
    <div onclick="bulkShareAppendToExisting('${t.code}')" style="display:flex;align-items:center;gap:12px;padding:12px;background:var(--surface2);border:1px solid var(--border);border-radius:10px;margin-bottom:8px;cursor:pointer" onmouseover="this.style.borderColor='var(--accent)'" onmouseout="this.style.borderColor='var(--border)'">
      <div style="width:32px;height:32px;border-radius:50%;background:${esc(t.colour||'var(--accent)')};display:flex;align-items:center;justify-content:center;color:#111;font-weight:700;font-size:13px;flex-shrink:0">${esc((t.name||'?').charAt(0).toUpperCase())}</div>
      <div style="flex:1;min-width:0">
        <div style="font-size:14px;font-weight:600;color:var(--text)">${esc(t.name)}</div>
        <div style="font-size:11px;color:var(--muted);font-family:var(--mono)">${esc(t.guestEmail||'(no email)')}</div>
      </div>
      <svg class="icon icon-sm" aria-hidden="true" style="color:var(--muted)"><use href="#i-plus"></use></svg>
    </div>`).join('') : `<p style="font-size:13px;color:var(--muted);font-style:italic;padding:8px 4px;margin:0">No existing shares yet.</p>`;

  overlay.innerHTML = `
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:16px;width:100%;max-width:480px;max-height:80vh;display:flex;flex-direction:column;box-shadow:0 12px 48px rgba(0,0,0,0.5)">
      <div style="padding:18px 18px 12px;border-bottom:1px solid var(--border)">
        <h3 style="font-size:17px;font-weight:700;margin:0 0 4px 0"><svg class="icon icon-lg" aria-hidden="true" style="color:var(--accent);vertical-align:-3px"><use href="#i-share-2"></use></svg> Share ${n} ${esc(noun)}</h3>
        <p style="font-size:12px;color:var(--muted);margin:0">Append to an existing share, or create a new one — recipients will see ONLY ${spec.pluralNoun==='items'?'these items':'this'}.</p>
      </div>
      <div style="flex:1;overflow-y:auto;padding:14px 18px">
        ${existing.length ? `<div style="font-size:11px;color:var(--muted);font-family:var(--mono);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px">Add to existing share</div>` : ''}
        ${existingRows}
        <button onclick="bulkShareCreateNew()" style="width:100%;padding:13px;border-radius:10px;border:1px dashed var(--accent);background:transparent;color:var(--accent);font-size:13px;font-weight:600;cursor:pointer;margin-top:8px;display:flex;align-items:center;justify-content:center;gap:6px">
          <svg class="icon" aria-hidden="true"><use href="#i-plus"></use></svg> Create new share with ${spec.pluralNoun==='items'?'these items':'this selection'}
        </button>
      </div>
      <div style="padding:12px 18px;border-top:1px solid var(--border);display:flex;justify-content:flex-end">
        <button class="btn btn-ghost btn-sm" onclick="document.getElementById('bulk-share-overlay').remove()">Cancel</button>
      </div>
    </div>`;
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.remove(); });
  document.body.appendChild(overlay);
}

// Ensure a share's SECTION permission for `sectionPermKey` is enabled (>= 'r')
// across all its households, and persist to the server. Called when records
// from a section are tagged into a share — without this the push filters the
// section out (canSee<Section> is false) and the guest never receives the
// records even though they're correctly allow-tagged (the "Ask home for
// access" / empty-section symptom). Idempotent: leaves an existing 'rw' alone,
// only upgrades 'none'/absent → 'r'. Updates _shareTargets locally first so the
// pushSharedData that follows sees the new perm.
async function _ensureShareSectionPerm(shareCode, sectionPermKey) {
  if (!sectionPermKey) return;
  const t = _shareTargets.find(s => s.code === shareCode);
  if (!t) return;
  const households = t.households || { default: {} };
  let changed = false;
  for (const hKey of Object.keys(households)) {
    const cur = households[hKey] && households[hKey][sectionPermKey];
    if (!cur || cur === 'none') {
      households[hKey] = { ...(households[hKey] || {}), [sectionPermKey]: 'r' };
      changed = true;
    }
  }
  if (!changed) return;
  t.households = households; // reflect locally so the next pushSharedData sees it
  const authFields = _kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier };
  const res = await postKV(`${WORKER_URL}/share/update`, {
    ownerEmailHash: _kvEmailHash, ...authFields, code: shareCode, households,
  });
  if (!res.ok) console.warn('[share] failed to persist section perm', sectionPermKey, 'on', shareCode);
  else console.log('[share] enabled section perm', sectionPermKey, 'on', shareCode);
}

// Merge shareCode into each selected record's share.allow. Items already
// explicitly denied to this share are skipped (count flagged in toast).
async function bulkShareAppendToExisting(shareCode) {
  document.getElementById('bulk-share-overlay')?.remove();
  const spec = _getActiveSpec();
  const set  = _getActiveSelection();
  if (!spec || !set || !set.size) return;
  const ids = [...set];
  const now = new Date().toISOString();
  let appended = 0;
  let alreadyDenied = 0;
  for (const id of ids) {
    const rec = spec.findRecord(id);
    if (!rec) continue;
    if (typeof rec.share === 'object' && Array.isArray(rec.share?.deny) && rec.share.deny.includes(shareCode)) {
      alreadyDenied++;
      continue;
    }
    if (rec.share == null) {
      rec.share = { allow: [shareCode] };
    } else if (typeof rec.share === 'object') {
      const allow = Array.isArray(rec.share.allow) ? rec.share.allow : [];
      if (!allow.includes(shareCode)) allow.push(shareCode);
      rec.share.allow = allow;
    } else if (rec.share === 'private') {
      rec.share = { allow: [shareCode] };
    }
    rec.updatedAt = now;
    // Cascade to children (e.g. grocery list → its items) — see
    // _applyBulkSharePending / the grocery spec's cascadeShare for why.
    if (typeof spec.cascadeShare === 'function') {
      try { spec.cascadeShare(rec, shareCode, now); } catch(e) { console.warn('[bulk-share] cascade failed:', e); }
    }
    appended++;
  }
  await spec.save();
  // Enable the section's permission on the share BEFORE pushing — without this
  // an appended record from a section the share didn't previously include
  // (e.g. adding a reminder/budget category to a grocery share) stays gated
  // off (canSee<Section> false) and the guest sees "Ask home for access".
  // (Same reason as the create path in _applyBulkSharePending.)
  if (appended > 0 && spec.sectionPermKey) {
    try { await _ensureShareSectionPerm(shareCode, spec.sectionPermKey); } catch(e) { console.warn('[bulk-share] section perm enable failed:', e); }
  }
  exitBulkSelectMode();
  if (spec.rerender) spec.rerender();
  _syncQueue?.enqueue?.('Updating sharing…');
  try { await pushSharedData(shareCode); } catch(_) {}
  const tgt = _shareTargets.find(t => t.code === shareCode);
  const recName = tgt?.name || 'share';
  const noun = (appended === 1) ? spec.noun : spec.pluralNoun;
  let msg = `Added ${appended} ${noun} to ${recName}`;
  if (alreadyDenied) msg += ` · ${alreadyDenied} skipped (explicitly denied)`;
  toast(msg);
}

// Pre-configures the share-target-modal for the bulk-share path: section
// perm set to 'r' so the recipient can see the selected records; other
// sections set to 'none'. Banner inserted, role/perms/mgmt rows hidden.
function bulkShareCreateNew() {
  document.getElementById('bulk-share-overlay')?.remove();
  const spec = _getActiveSpec();
  const set  = _getActiveSelection();
  if (!spec || !set || !set.size) return;
  // Capture the selection AND the active section — exitBulkSelectMode
  // clears the live state, so saveShareTarget needs both pieces.
  const _pendingSelection = {
    section: _bulkSelectActiveSection,
    ids: [...set],
  };
  // openAddShareTarget clears _bulkShareSelectionPending (to stop an abandoned
  // selection from making a plain "Add Person" share record-scoped). So we
  // MUST assign our legitimate pending selection AFTER that call — otherwise
  // openAddShareTarget wipes it and the banner code below reads null.ids and
  // crashes the "Create & get link" button.
  openAddShareTarget();
  _bulkShareSelectionPending = _pendingSelection;
  setTimeout(() => {
    const targetPerm = spec.sectionPermKey;
    const hhKeys = Object.keys(_shareTargetPerms || {});
    const blankPerms = { stockroom: 'none', groceries: 'none', reminders: 'none', budget: 'none' };
    if (!hhKeys.length) {
      _shareTargetPerms = { default: { ...blankPerms, [targetPerm]: 'r' } };
    } else {
      for (const k of hhKeys) {
        _shareTargetPerms[k] = { ...blankPerms, [targetPerm]: 'r' };
      }
    }
    if (typeof renderShareHouseholdPerms === 'function') renderShareHouseholdPerms();
    // Hide the sections that don't apply to "share these specific records"
    const _row1 = document.getElementById('share-target-role-row');
    const _row2 = document.getElementById('share-target-perms-row');
    const _row3 = document.getElementById('share-target-mgmt-row');
    if (_row1) _row1.style.display = 'none';
    if (_row2) _row2.style.display = 'none';
    if (_row3) _row3.style.display = 'none';
    _shareTargetMgmt = 'none';
    // Banner
    const modal = document.getElementById('share-target-modal');
    let banner = document.getElementById('bulk-share-pending-banner');
    if (modal && !banner && _bulkShareSelectionPending) {
      const n = _bulkShareSelectionPending.ids.length;
      const nounLabel = (n === 1) ? spec.noun : spec.pluralNoun;
      banner = document.createElement('div');
      banner.id = 'bulk-share-pending-banner';
      banner.style.cssText = 'margin:0 0 12px 0;padding:10px 12px;background:rgba(232,168,56,0.08);border:1px solid rgba(232,168,56,0.3);border-radius:8px;font-size:12px;color:var(--text);line-height:1.4';
      banner.innerHTML = `<svg class="icon icon-sm" aria-hidden="true" style="color:var(--accent);vertical-align:-2px"><use href="#i-share-2"></use></svg> Sharing <strong>${n} selected ${nounLabel}</strong> with one person. Enter their email below (they must have a STOCKROOM account) — they'll see only ${spec.pluralNoun==='items'?'these items':'this selection'}.`;
      const modalEl = modal.querySelector('.modal');
      if (modalEl) modalEl.insertBefore(banner, modalEl.firstChild?.nextSibling || null);
    }
  }, 30);
}

// Pending bulk-share state — set by bulkShareCreateNew, consumed by
// saveShareTarget after a successful create. Cleared on cancel or save.
let _bulkShareSelectionPending = null;

// Cancel path for the share-target-modal. Clears bulk-share pending so
// the next normal share-create doesn't accidentally apply the previous
// pending allow-list.
function _cancelShareTargetModal() {
  _bulkShareSelectionPending = null;
  document.getElementById('bulk-share-pending-banner')?.remove();
  closeModal('share-target-modal');
}

// Apply pending bulk-share allow-list overrides. Called from
// saveShareTarget after a new share is created and data.code is known.
// Dispatches to the right section spec — works for stock, groceries,
// reminders, transactions, etc — because the spec carries findRecord/save.
async function _applyBulkSharePending(newShareCode) {
  if (!_bulkShareSelectionPending) return;
  const { ids, section } = _bulkShareSelectionPending;
  _bulkShareSelectionPending = null;
  if (!Array.isArray(ids) || !ids.length) return;
  const spec = _BULK_SELECT_SECTIONS[section];
  if (!spec) { console.warn('[bulk-share] unknown section for pending:', section); return; }
  const now = new Date().toISOString();
  for (const id of ids) {
    const rec = spec.findRecord(id);
    if (!rec) continue;
    if (rec.share == null) {
      rec.share = { allow: [newShareCode] };
    } else if (typeof rec.share === 'object') {
      const allow = Array.isArray(rec.share.allow) ? rec.share.allow : [];
      if (!allow.includes(newShareCode)) allow.push(newShareCode);
      rec.share.allow = allow;
    } else if (rec.share === 'private') {
      rec.share = { allow: [newShareCode] };
    }
    rec.updatedAt = now;
    // Cascade hook: some sections share a CONTAINER whose children must also
    // be tagged. A grocery LIST, for example, is the selectable unit, but the
    // share filter operates on grocery ITEMS — tagging only the list leaves a
    // record-scoped share with an empty list. The spec's cascadeShare tags the
    // children too. (Mirrors applyDelete's cascade-to-items behaviour.)
    if (typeof spec.cascadeShare === 'function') {
      try { spec.cascadeShare(rec, newShareCode, now); } catch(e) { console.warn('[bulk-share] cascade failed:', e); }
    }
  }
  await spec.save();
  // Ensure the share's section permission for this section is enabled, so the
  // pushed records are actually visible to the guest. The create flow seeds a
  // perm for the create-section, but enabling here is idempotent and also
  // covers the case where the share spans sections. (Same reason as the
  // append path — see _ensureShareSectionPerm.)
  if (spec.sectionPermKey) {
    try { await _ensureShareSectionPerm(newShareCode, spec.sectionPermKey); } catch(e) { console.warn('[bulk-share] section perm enable failed:', e); }
  }
  if (spec.rerender) spec.rerender();
  try { await pushSharedData(newShareCode); } catch(_) {}
  const noun = (ids.length === 1) ? spec.noun : spec.pluralNoun;
  toast(`✓ Shared ${ids.length} ${noun}`);
}

function handleShareTargetBtn() {
  if (_shareTargetDone) {
    closeModal('share-target-modal');
  } else {
    saveShareTarget();
  }
}

function renderShareAccessControl() {
  const sac = _ensureShareAccessControl();
  const summary = document.getElementById('sac-summary');
  if (summary) {
    const denyCount  = sac.deny.length;
    const allowCount = sac.allow.length;
    if (sac.mode === 'allowlist') {
      summary.textContent = `Allowlist mode · ${allowCount} allowed · ${denyCount} blocked`;
    } else {
      summary.textContent = `Open mode · ${denyCount} blocked`;
    }
  }
  // Mode radios
  const openRadio  = document.getElementById('sac-mode-open');
  const alistRadio = document.getElementById('sac-mode-allowlist');
  if (openRadio)  openRadio.checked  = (sac.mode === 'open');
  if (alistRadio) alistRadio.checked = (sac.mode === 'allowlist');
  // Mode help text
  const helpEl = document.getElementById('sac-mode-help');
  if (helpEl) {
    helpEl.textContent = sac.mode === 'allowlist'
      ? 'Only emails on the allow list can be added to a share.'
      : 'Anyone can be added except entries on the deny list.';
  }
  // Allow section visibility
  const allowSec = document.getElementById('sac-allow-section');
  if (allowSec) allowSec.style.display = (sac.mode === 'allowlist') ? '' : 'none';
  // Deny chips
  _renderShareAccessChips('deny', sac.deny);
  // Allow chips
  _renderShareAccessChips('allow', sac.allow);
  // Existing-share conflict hint — list any current share targets whose
  // email is on the deny list. Helps the owner clean up after adding
  // someone to deny *after* an existing share was created (forward-only
  // enforcement, see plan doc).
  const hintEl = document.getElementById('sac-conflict-hint');
  if (hintEl && Array.isArray(_shareTargets) && _shareTargets.length) {
    const denySet = new Set(sac.deny.map(_normEmail));
    const conflicts = _shareTargets.filter(t => t.guestEmail && denySet.has(_normEmail(t.guestEmail)));
    if (conflicts.length) {
      const names = conflicts.map(t => esc(t.name || t.guestEmail)).join(', ');
      hintEl.innerHTML = `<strong>⚠ ${conflicts.length} existing share${conflicts.length===1?'':'s'}</strong> use an email that's on your deny list (${names}). The share${conflicts.length===1?'':'s'} will keep working until you remove them manually below.`;
      hintEl.style.display = '';
    } else {
      hintEl.style.display = 'none';
    }
  } else if (hintEl) {
    hintEl.style.display = 'none';
  }
}

function _renderShareAccessChips(listKey, entries) {
  const slot = document.getElementById(`sac-${listKey}-chips`);
  if (!slot) return;
  if (!entries.length) {
    slot.innerHTML = `<div style="font-size:11px;color:var(--muted);font-style:italic">No entries yet.</div>`;
    return;
  }
  slot.innerHTML = entries.map(email => `
    <div style="display:inline-flex;align-items:center;gap:6px;background:var(--surface2);border:1px solid var(--border);border-radius:99px;padding:4px 4px 4px 10px;font-size:12px">
      <span>${esc(email)}</span>
      <button onclick="removeShareAccessEntry('${listKey}', '${esc(email).replace(/'/g, "\\'")}')"
        aria-label="Remove ${esc(email)}"
        style="background:transparent;border:none;cursor:pointer;color:var(--muted);padding:2px 6px;border-radius:99px;display:flex;align-items:center"
        onmouseover="this.style.color='var(--danger)'" onmouseout="this.style.color='var(--muted)'">
        <svg class="icon icon-sm" aria-hidden="true"><use href="#i-x"></use></svg>
      </button>
    </div>
  `).join('');
}

function toggleShareAccessControlPanel() {
  const body = document.getElementById('share-access-control-body');
  const chev = document.getElementById('sac-chevron');
  if (!body) return;
  const open = body.style.display === 'none' || !body.style.display;
  body.style.display = open ? '' : 'none';
  if (chev) chev.style.transform = open ? 'rotate(180deg)' : '';
  if (open) renderShareAccessControl();
}

async function setShareAccessControlMode(mode) {
  const sac = _ensureShareAccessControl();
  sac.mode = (mode === 'allowlist') ? 'allowlist' : 'open';
  sac.updatedAt = new Date().toISOString();
  await _saveSettings();
  renderShareAccessControl();
}

function onShareAccessInputKey(e, listKey) {
  if (e.key === 'Enter') {
    e.preventDefault();
    addShareAccessEntry(listKey);
  }
}

async function addShareAccessEntry(listKey) {
  const input = document.getElementById(`sac-${listKey}-input`);
  const errEl = document.getElementById(`sac-${listKey}-error`);
  if (!input) return;
  const raw = input.value;
  const email = _normEmail(raw);
  // Basic validation. The same regex pattern the share-create modal uses
  // implicitly via type="email" + form-validation — kept loose deliberately
  // (proper RFC 5322 validation is famously not worth doing).
  const emailLooksValid = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
  if (!emailLooksValid) {
    if (errEl) errEl.textContent = 'Not a valid email address';
    return;
  }
  const sac = _ensureShareAccessControl();
  const list = listKey === 'allow' ? sac.allow : sac.deny;
  // Duplicate detection — case-insensitive
  if (list.map(_normEmail).includes(email)) {
    if (errEl) errEl.textContent = 'Already on this list';
    return;
  }
  // Cross-list conflict — same email can't sit on both deny AND allow.
  // Deny always wins, so if we're adding to allow but it's on deny, refuse.
  const otherKey = listKey === 'allow' ? 'deny' : 'allow';
  const otherList = listKey === 'allow' ? sac.deny : sac.allow;
  if (otherList.map(_normEmail).includes(email)) {
    if (errEl) errEl.textContent = `Already on the ${otherKey} list — remove it from there first`;
    return;
  }
  list.push(email);
  sac.updatedAt = new Date().toISOString();
  input.value = '';
  if (errEl) errEl.textContent = '';
  await _saveSettings();
  renderShareAccessControl();
}

async function removeShareAccessEntry(listKey, email) {
  const sac = _ensureShareAccessControl();
  const list = listKey === 'allow' ? sac.allow : sac.deny;
  const targetNorm = _normEmail(email);
  const before = list.length;
  if (listKey === 'allow') {
    sac.allow = list.filter(e => _normEmail(e) !== targetNorm);
  } else {
    sac.deny = list.filter(e => _normEmail(e) !== targetNorm);
  }
  if (sac.allow.length === before && sac.deny.length === before) return;
  sac.updatedAt = new Date().toISOString();
  await _saveSettings();
  renderShareAccessControl();
}

// Live access-control feedback inside the share-create / share-edit modal.
// Wired on the email input's `oninput` + `onblur`. Renders the deny/allow
// reason inline so the user sees the error before clicking Save.
function onShareTargetEmailInput() {
  const input  = document.getElementById('share-target-email');
  const errEl  = document.getElementById('share-target-email-sac-error');
  if (!input || !errEl) return;
  const val = (input.value || '').trim();
  if (!val) { errEl.textContent = ''; return; }
  // Only check once it looks like a complete email — partial typing
  // ("alice@" etc.) shouldn't flash an error before they've finished.
  const looksValid = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(val);
  if (!looksValid) { errEl.textContent = ''; return; }
  const check = checkShareAccessControl(val);
  errEl.textContent = check.ok ? '' : check.reason;
}

function renderShareTargetsList() {
  const list = document.getElementById('share-targets-list');
  const btn  = document.getElementById('add-share-target-btn');
  const section = document.getElementById('share-targets-section');
  if (!list) return;

  // Visibility gate: show the section if the user has any owned shares OR
  // has view/edit shareManagement permission on a share they joined as a
  // guest. The previous check (canViewShares alone) hid the entire section
  // for users who own shares AND are also a guest in another household,
  // which is a normal multi-tenant scenario. The owned-shares list comes
  // from _shareTargets (populated by loadShareTargets — the server only
  // returns shares the caller owns), so its non-emptiness is a reliable
  // signal of ownership.
  const hasOwnedShares = Array.isArray(_shareTargets) && _shareTargets.length > 0;
  const viewable = hasOwnedShares || canViewShares();
  if (!viewable) {
    if (section) section.style.display = 'none';
    return;
  }
  if (section) section.style.display = '';

  // Treat the user as effective owner of the Share Access section if they
  // either are not in guest mode at all, OR they have at least one owned
  // share. Without this, an owner-who-also-joined-another-share would see
  // their own shares but with all action buttons hidden (canManageShares
  // returns false because canViewShares only checks guest permissions).
  const editable = hasOwnedShares || canManageShares();

  // Add-person button is hidden in view-only mode
  if (btn) btn.style.display = (editable && _shareTargets.length < 5) ? 'inline-flex' : 'none';

  const typeEmoji = { family: '👨‍👩‍👧', cleaner: '🧹', guest: '👤' };
  if (!_shareTargets.length) {
    list.innerHTML = `<p style="font-size:12px;color:var(--muted)">No one has access yet.</p>`;
  } else {
    list.innerHTML = _shareTargets.map(t => {
      const colour    = t.colour || '#e8a838';
      const memberDetails = t.memberDetails || {};
      // Split members into active vs pending. A member is pending only when
      // memberDetails[hash].status === 'pending'; everything else (active, or
      // legacy with no status) is a real member. Pending requesters sit in
      // t.members too, so we must exclude them from the Members list/count.
      const allMembers = Array.isArray(t.members) ? t.members : [];
      const activeMembers = allMembers.filter(h => (memberDetails[h]?.status || 'active') !== 'pending' && (memberDetails[h]?.status || 'active') !== 'rejected');
      const members   = activeMembers.length;
      // Pending requests come folded in from /share/list (with resolved email
      // + pubkey). Fall back to deriving from memberDetails if absent.
      const pendingReqs = Array.isArray(t.pendingRequests) && t.pendingRequests.length
        ? t.pendingRequests
        : allMembers.filter(h => memberDetails[h]?.status === 'pending')
            .map(h => ({ guestEmailHash: h, email: null, requestedAt: memberDetails[h]?.requestedAt || null, guestPublicKeyJwk: null }));
      const expired   = t.expiresAt && Date.now() > new Date(t.expiresAt).getTime();
      const expiryStr = t.expiresAt ? (expired ? '<svg class="icon" aria-hidden="true"><use href="#i-alert-triangle"></use></svg> Link expired' : `Link valid until ${new Date(t.expiresAt).toLocaleTimeString('en-GB',{hour:'2-digit',minute:'2-digit'})}`) : '';
      const isExpanded = !!_expandedShareCodes[t.code];

      // Member sub-rows — only meaningful when there are joined members.
      // Each row shows the member's email-hash prefix (we don't store the
      // raw email server-side for privacy), last-active relative time,
      // and a "Remove" button (mgmt-edit only).
      const memberRows = members ? `
        <div style="margin-top:10px;padding-top:10px;border-top:1px solid var(--border);display:${isExpanded?'block':'none'}" id="share-members-${t.code}">
          <div style="font-size:11px;color:var(--muted);margin-bottom:8px;text-transform:uppercase;letter-spacing:0.5px">Members</div>
          ${activeMembers.map(memberHash => {
            const md = memberDetails[memberHash] || {};
            const lastActive = md.lastActiveAt ? _relTime(md.lastActiveAt) : 'Not yet active';
            const firstSeen  = md.firstSeenAt  ? new Date(md.firstSeenAt).toLocaleDateString() : '—';
            const pulls      = md.pullCount || 0;
            return `<div style="display:flex;align-items:center;gap:8px;padding:6px 8px;background:var(--bg);border-radius:8px;margin-bottom:4px">
              <svg class="icon icon-sm" aria-hidden="true" style="color:var(--muted);flex-shrink:0"><use href="#i-user"></use></svg>
              <div style="flex:1;min-width:0">
                <div style="font-size:11px;font-family:var(--mono);color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(memberHash.slice(0,12))}…</div>
                <div style="font-size:10px;color:var(--muted);margin-top:1px">
                  <svg class="icon" aria-hidden="true" style="width:10px;height:10px;vertical-align:-1px"><use href="#i-clock"></use></svg>
                  ${esc(lastActive)} · joined ${esc(firstSeen)}${pulls>0?` · ${pulls} sync${pulls===1?'':'s'}`:''}
                </div>
              </div>
              ${editable ? `<button class="btn btn-ghost btn-sm" style="color:var(--danger);padding:4px 8px;font-size:11px" onclick="removeShareMember('${t.code}','${memberHash}')" title="Remove this member"><svg class="icon" aria-hidden="true"><use href="#i-x"></use></svg></button>` : ''}
            </div>`;
          }).join('')}
        </div>` : '';

      // ── Pending requests section ─────────────────────────────
      // People who followed the link and are waiting for approval. Owner-only
      // (editable). Always visible (not gated by the members expand toggle) so
      // a request is impossible to miss. Approve wraps the share key in the
      // owner's session; Reject declines + revokes.
      const pendingRows = (editable && pendingReqs.length) ? `
        <div id="share-pending-${t.code}" style="margin-top:10px;padding-top:10px;border-top:1px solid var(--accent)">
          <div style="font-size:11px;color:var(--accent);margin-bottom:8px;text-transform:uppercase;letter-spacing:0.5px;font-weight:700">
            <svg class="icon" aria-hidden="true" style="width:11px;height:11px;vertical-align:-1px"><use href="#i-clock"></use></svg>
            Pending requests (${pendingReqs.length})
          </div>
          ${pendingReqs.map(pr => {
            const who = pr.email ? esc(pr.email) : esc((pr.guestEmailHash||'').slice(0,12)) + '…';
            const when = pr.requestedAt ? _relTime(pr.requestedAt) : '';
            const hasKey = !!pr.guestPublicKeyJwk;
            return `<div style="display:flex;align-items:center;gap:8px;padding:8px;background:var(--bg);border:1px solid var(--accent);border-radius:8px;margin-bottom:6px">
              <svg class="icon icon-sm" aria-hidden="true" style="color:var(--accent);flex-shrink:0"><use href="#i-user"></use></svg>
              <div style="flex:1;min-width:0">
                <div style="font-size:12px;color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${who}</div>
                <div style="font-size:10px;color:var(--muted);margin-top:1px">Requested${when?' '+esc(when):''}${hasKey?'':' · key pending'}</div>
              </div>
              <button class="btn btn-primary btn-sm" style="padding:5px 12px;font-size:11px"${hasKey?'':' disabled'} onclick="_approveShareRequestUI('${t.code}','${pr.guestEmailHash}')" title="Approve — grant access">Approve</button>
              <button class="btn btn-ghost btn-sm" style="color:var(--danger);padding:5px 10px;font-size:11px" onclick="_rejectShareRequestUI('${t.code}','${pr.guestEmailHash}')" title="Decline this request">Decline</button>
            </div>`;
          }).join('')}
        </div>` : '';

      // Action buttons — full set for owners/edit, none for view-only
      const actionsHtml = editable ? `
        <button class="btn btn-ghost btn-sm" onclick="openEditShareTarget('${t.code}')" title="Edit"><svg class="icon" aria-hidden="true"><use href="#i-pencil"></use></svg></button>
        ${expired
          ? `<button class="btn btn-ghost btn-sm" onclick="refreshShareLink('${t.code}')" title="Refresh link (new 24h window)"><svg class="icon" aria-hidden="true"><use href="#i-refresh-cw"></use></svg></button>`
          : `<button class="btn btn-ghost btn-sm" onclick="copyShareTargetLink('${t.code}')" title="Copy invite link">🔗</button>`
        }
        <button class="btn btn-ghost btn-sm" onclick="resyncSharedData('${t.code}')" title="Re-sync data to guest"><svg class="icon" aria-hidden="true"><use href="#i-share-2"></use></svg></button>
        <button class="btn btn-ghost btn-sm" style="color:var(--danger)" onclick="deleteShareTarget('${t.code}')" title="Remove share"><svg class="icon" aria-hidden="true"><use href="#i-x"></use></svg></button>
      ` : '';

      // Expand/collapse chevron — always present when there are members so
      // viewers can audit who has access even in view-only mode.
      const expandBtn = members ? `<button class="btn btn-ghost btn-sm" onclick="toggleShareMembers('${t.code}')" title="${isExpanded?'Hide':'Show'} members" aria-expanded="${isExpanded}"><svg class="icon" aria-hidden="true" style="transform:rotate(${isExpanded?180:0}deg);transition:transform 0.15s"><use href="#i-chevron-down"></use></svg></button>` : '';

      return `
      <div style="display:flex;flex-direction:column;padding:10px 12px;background:var(--surface2);border:1px solid ${expired?'var(--danger)':'var(--border)'};border-radius:10px">
        <div style="display:flex;align-items:center;gap:10px">
          <div style="width:12px;height:12px;border-radius:50%;background:${colour};flex-shrink:0;box-shadow:0 1px 4px rgba(0,0,0,0.3)"></div>
          <div style="flex:1;min-width:0">
            <div style="font-size:13px;font-weight:700">${typeEmoji[t.type]||'👤'} ${esc(t.name)}</div>
            <div style="font-size:11px;color:var(--muted);font-family:var(--mono)">${t.type}${members?' · '+members+' member'+(members!==1?'s':''):''}${(editable && pendingReqs.length)?` · <span style="color:var(--accent);font-weight:700">${pendingReqs.length} pending</span>`:''}${t.shareManagement && t.shareManagement !== 'none' ? ' · share-'+t.shareManagement : ''}</div>
            ${expiryStr?`<div style="font-size:10px;color:${expired?'var(--danger)':'var(--muted)'};margin-top:2px">${expiryStr}</div>`:''}
          </div>
          ${expandBtn}
          ${actionsHtml}
        </div>
        ${pendingRows}
        ${memberRows}
      </div>`;
    }).join('');
  }
  const clearBtn = document.getElementById('clear-all-shares-btn');
  if (clearBtn) clearBtn.style.display = (editable && _shareTargets.length > 0) ? 'inline-flex' : 'none';
  // Refresh the Share Access Control summary alongside — it lives in the
  // same Settings → Household Sharing section, and its conflict hint
  // depends on the share-targets list we just refreshed above.
  try { renderShareAccessControl(); } catch (_) {}
}

// Toggle expand/collapse for a share row's member list. Survives
// re-renders within a session but resets on page reload.
function toggleShareMembers(code) {
  _expandedShareCodes[code] = !_expandedShareCodes[code];
  renderShareTargetsList();
}

// ── Approve a pending request (button handler) ──────────────────
// Looks up the requester's pubkey from the folded pendingRequests, delegates
// the ECDH wrap + POST to _approveShareRequest (app.js), then refreshes.
async function _approveShareRequestUI(code, guestEmailHash) {
  const t = (_shareTargets || []).find(x => x.code === code);
  const pr = t && Array.isArray(t.pendingRequests)
    ? t.pendingRequests.find(p => p.guestEmailHash === guestEmailHash) : null;
  const pubKey = pr ? pr.guestPublicKeyJwk : null;
  if (!pubKey) { toast('Cannot approve yet — requester key not available'); return; }
  if (typeof _approveShareRequest !== 'function') { toast('Approve unavailable — please reload'); return; }
  toast('Approving…');
  const ok = await _approveShareRequest(code, guestEmailHash, pubKey);
  if (ok) {
    toast('Approved ✓');
    if (typeof loadShareTargets === 'function') await loadShareTargets();
    else renderShareTargetsList();
  }
}

// ── Decline a pending request (button handler) ──────────────────
async function _rejectShareRequestUI(code, guestEmailHash) {
  if (!confirm('Decline this access request? They will be notified they can request again.')) return;
  if (typeof _rejectShareRequest !== 'function') { toast('Decline unavailable — please reload'); return; }
  const ok = await _rejectShareRequest(code, guestEmailHash);
  if (ok) {
    toast('Request declined');
    if (typeof loadShareTargets === 'function') await loadShareTargets();
    else renderShareTargetsList();
  }
}

// Human-readable relative time ("3 mins ago", "2 hours ago", etc).
// Used for last-active timestamps in the member list.
function _relTime(iso) {
  if (!iso) return '—';
  const ms = Date.now() - new Date(iso).getTime();
  if (ms < 0) return 'just now';
  const s = Math.floor(ms/1000);
  if (s < 60)   return 'just now';
  const m = Math.floor(s/60);
  if (m < 60)   return `${m} min${m===1?'':'s'} ago`;
  const h = Math.floor(m/60);
  if (h < 24)   return `${h} hour${h===1?'':'s'} ago`;
  const d = Math.floor(h/24);
  if (d < 30)   return `${d} day${d===1?'':'s'} ago`;
  return new Date(iso).toLocaleDateString();
}

// Surgically remove a single member from a share. Calls the
// /share/member/remove endpoint which evicts them, drops their wrapped
// share key, and writes a 7-day revocation marker so their next pull
// returns 403 fast and they self-clean.
async function removeShareMember(code, memberHash) {
  const target = _shareTargets.find(t => t.code === code);
  if (!target) return;
  const memberLabel = memberHash.slice(0, 12) + '…';
  if (!confirm(`Remove this member (${memberLabel}) from "${target.name}"?\n\nThey'll lose access immediately on their next sync, and their copy of the shared data will clear from their device. Other members of this share are unaffected.`)) return;
  try {
    const authFields = _kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier };
    const res = await postKV(`${WORKER_URL}/share/member/remove`, {
      ownerEmailHash: _kvEmailHash, ...authFields, code, guestEmailHash: memberHash,
    });
    if (!res.ok) {
      const d = await _readJsonSafe(res) || {};
      throw new Error(d.error || 'Could not remove member');
    }
    toast('Member removed ✓');
    await loadShareTargets();
  } catch(err) {
    toast('Could not remove member: ' + err.message);
  }
}

// Render the three-button None / View / Edit selector for share-management.
// Lives in a dedicated region above the per-household perms grid. If the
// host HTML doesn't include a #share-mgmt-perm element, this auto-injects
// one above #share-household-perms so the picker still renders.
function renderShareMgmtPicker() {
  let el = document.getElementById('share-mgmt-perm');
  if (!el) {
    const anchor = document.getElementById('share-household-perms');
    if (!anchor || !anchor.parentNode) return;
    const wrap = document.createElement('div');
    wrap.style.cssText = 'margin-bottom:10px';
    wrap.innerHTML = `
      <div style="font-size:11px;color:var(--muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:0.5px">Share-access management</div>
      <div id="share-mgmt-perm" style="display:flex;gap:6px;flex-wrap:wrap"></div>`;
    anchor.parentNode.insertBefore(wrap, anchor);
    el = document.getElementById('share-mgmt-perm');
    if (!el) return;
  }
  const opts = [
    { v: 'none', label: '🔒 None', hint: 'Cannot see share access' },
    { v: 'view', label: '👁 View', hint: 'Can see who has access (read-only)' },
    { v: 'edit', label: '✏️ Edit', hint: 'Can add, edit, remove share access' },
  ];
  el.innerHTML = opts.map(o => {
    const active = o.v === _shareTargetMgmt;
    return `<button onclick="setShareMgmt('${o.v}')" type="button"
      style="flex:1;min-width:90px;padding:8px 10px;border-radius:8px;cursor:pointer;font-size:12px;
             border:1px solid ${active?'var(--accent)':'var(--border)'};
             background:${active?'rgba(232,168,56,0.15)':'transparent'};
             color:${active?'var(--accent)':'var(--muted)'};transition:all 0.15s;
             display:flex;flex-direction:column;align-items:flex-start;gap:2px;text-align:left">
      <span style="font-weight:600">${o.label}</span>
      <span style="font-size:10px;color:var(--muted);font-weight:400">${o.hint}</span>
    </button>`;
  }).join('');
}

function setShareMgmt(v) {
  _shareTargetMgmt = v;
  renderShareMgmtPicker();
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
  // Reset share-management permission to type default (always 'none' today —
  // there's no per-type override yet, but keeping the indirection makes
  // future tweaks one-line).
  _shareTargetMgmt = SHARE_MGMT_DEFAULTS[type] || 'none';
  renderShareMgmtPicker();
  renderShareHouseholdPerms();
}

async function renderShareHouseholdPerms() {
  const container = document.getElementById('share-household-perms');
  if (!container) return;
  const profiles  = await getProfiles();
  const sections  = ['stockroom','groceries','reminders','savings','report','budget'];
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
  // Owners (not in guest mode at all) can always add. Users in guest mode
  // can add IF they have shareManagement edit permission on the share they
  // joined, OR they already own shares of their own (the typical case for a
  // returning user who's a tenant on another household and also runs their
  // own). Without the hasOwnedShares clause, an owner who happened to join
  // another share would see their own Share Access section but be unable
  // to add new entries to it.
  const hasOwnedShares = Array.isArray(_shareTargets) && _shareTargets.length > 0;
  if (!isOwner() && !canManageShares() && !hasOwnedShares) {
    toast('Only the household owner can manage share access');
    return;
  }
  if (_shareTargets.length >= 5) { toast('Maximum 5 share targets reached'); return; }
  // Clear any stale bulk-share selection. If the user previously started a
  // "share these specific items" flow and abandoned it (closed the modal
  // without completing), _bulkShareSelectionPending could still be set — and
  // saveShareTarget reads it to decide recordScoped. An abandoned selection
  // would silently make this plain "Add Person" share record-scoped (sections
  // enabled but no records visible). A normal Add Person is always a SECTION
  // share, so clear the pending selection here.
  _bulkShareSelectionPending = null;
  _shareTargetType   = 'family';
  _shareTargetPerms  = {};
  _shareTargetColour = HOUSEHOLD_COLOURS[_shareTargets.length % HOUSEHOLD_COLOURS.length];
  _shareTargetMgmt   = SHARE_MGMT_DEFAULTS.family;
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
  // Clear any lingering deny/allow inline error from a previous open
  const _sacErr = document.getElementById('share-target-email-sac-error');
  if (_sacErr) _sacErr.textContent = '';
  // Hide the "send notification" checkbox — only relevant on edit
  const sendEmailRow = document.getElementById('share-send-email-row');
  if (sendEmailRow) sendEmailRow.style.display = 'none';
  hide('share-link-section');
  document.getElementById('share-target-save-btn').textContent = 'Create & get link';
  // Restore the three sections that bulk-share mode hides (Role / Access
  // per household / Share management). When openAddShareTarget runs from
  // the normal Settings → Add Person path, these need to be visible. The
  // bulk-share flow re-hides them in bulkShareCreateNew AFTER this
  // function returns, so the order is: reset everything → hide as needed.
  const _row1 = document.getElementById('share-target-role-row');
  const _row2 = document.getElementById('share-target-perms-row');
  const _row3 = document.getElementById('share-target-mgmt-row');
  if (_row1) _row1.style.display = '';
  if (_row2) _row2.style.display = '';
  if (_row3) _row3.style.display = '';
  // Also clear any leftover bulk-share banner from a prior invocation.
  document.getElementById('bulk-share-pending-banner')?.remove();
  selectShareType('family', document.querySelector('.share-type-btn[data-type="family"]'));
  renderShareTargetColourPicker(_shareTargetColour);
  renderShareMgmtPicker();
  await renderShareHouseholdPerms();
  openModal('share-target-modal');
}

async function openEditShareTarget(code) {
  const target = _shareTargets.find(t => t.code === code);
  if (!target) return;
  _shareTargetType   = target.type || 'family';
  _shareTargetPerms  = JSON.parse(JSON.stringify(target.households || {}));
  _shareTargetColour = target.colour || HOUSEHOLD_COLOURS[0];
  _shareTargetMgmt   = target.shareManagement || 'none';
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

  hide('share-link-section');
  document.getElementById('share-target-save-btn').textContent = 'Save changes';
  selectShareType(_shareTargetType, document.querySelector(`.share-type-btn[data-type="${_shareTargetType}"]`));
  // selectShareType resets _shareTargetMgmt to type default; restore the
  // saved value AFTER so the editor shows what's actually stored.
  _shareTargetMgmt = target.shareManagement || 'none';
  renderShareTargetColourPicker(_shareTargetColour);
  renderShareMgmtPicker();
  await renderShareHouseholdPerms();
  openModal('share-target-modal');
}

async function saveShareTarget() {
  const nameEl = document.getElementById('share-target-name');
  if (!nameEl) { console.error('saveShareTarget: name field not found in DOM'); toast('Form not ready — reopen the dialog'); return; }
  const name = nameEl.value.trim();
  if (!name) { toast('Enter a name for this person'); nameEl.focus?.(); return; }
  const emailEl = document.getElementById('share-target-email');
  const emailVal = emailEl?.value.trim() || '';
  const codeEl   = document.getElementById('share-target-code');
  const code     = codeEl ? codeEl.value : '';
  // Up-front guard for the create path: grant->accept requires an email so we
  // can wrap the key for that specific account. Fail loudly and visibly rather
  // than deep inside the try (where a missed toast looked like "nothing happened").
  if (!code && !emailVal) {
    console.warn('[share] create blocked — no recipient email entered');
    toast("Enter the person's email address - access is granted to a specific account");
    if (emailEl) {
      emailEl.style.borderColor = 'var(--danger, #e35)';
      emailEl.scrollIntoView({ block: 'center', behavior: 'smooth' });
      emailEl.focus?.();
      // Clear the red flag once they start typing.
      const clear = () => { emailEl.style.borderColor = ''; emailEl.removeEventListener('input', clear); };
      emailEl.addEventListener('input', clear);
    }
    return;
  }
  const colour    = _shareTargetColour;
  const profiles  = await getProfiles();
  const ownerName = settings.email?.split('@')[0] || profiles['default']?.name || 'Home';
  const btn = document.getElementById('share-target-save-btn');
  if (btn) { btn.textContent = '\u23f3'; btn.disabled = true; }
  console.log('[share] saveShareTarget start - code:', code || '(new)', 'email:', emailVal || '(none)');

  try {
    if (code) {
      // Update existing — re-use existing share key.
      // Deny / allow check: only fires if the email is being CHANGED to a
      // newly-restricted value. Editing other fields on an existing share
      // whose email is already denied is fine — the share exists, the
      // owner is allowed to keep it (forward-only enforcement). The
      // renderShareAccessControl conflict hint already nudges them to
      // remove these shares manually if they want.
      const _currentTgt    = _shareTargets.find(t => t.code === code);
      const _existingEmail = _normEmail(_currentTgt?.guestEmail || '');
      const _editingEmail  = _normEmail(document.getElementById('share-target-email')?.value || '');
      if (_editingEmail && _editingEmail !== _existingEmail) {
        const sacCheck = checkShareAccessControl(_editingEmail);
        if (!sacCheck.ok) { if (btn) { btn.textContent = 'Save changes'; btn.disabled = false; } toast(sacCheck.reason); return; }
      }
      // Update existing — re-use existing share key
      const res = await postKV(`${WORKER_URL}/share/update`, { ownerEmailHash: _kvEmailHash, verifier: _kvVerifier, sessionToken: _kvSessionToken, code, name, type: _shareTargetType, colour, households: _shareTargetPerms, shareManagement: _shareTargetMgmt });
      if (!res.ok) { const d = await res.json().catch(()=>({})); throw new Error(d.error || 'Update failed'); }
      // CRITICAL: reflect the new permissions in the local _shareTargets entry
      // BEFORE pushing. pushSharedData reads _shareTargets to decide which
      // sections to include (canSeeGroceries / canSeeReminders / …). Without
      // this, the push below runs with the STALE pre-edit perms (loadShareTargets
      // doesn't refresh until later), so newly-enabled sections push empty and
      // the guest never receives them — exactly the "section unlocks but no data"
      // symptom. Mutating the in-memory target makes the push see the new perms.
      const _editedTgt = _shareTargets.find(t => t.code === code);
      if (_editedTgt) {
        _editedTgt.households = JSON.parse(JSON.stringify(_shareTargetPerms));
        _editedTgt.shareManagement = _shareTargetMgmt;
      }
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
      // Create new — GRANT→ACCEPT model. The owner grants a SPECIFIC person
      // access here: we generate the share key and wrap it for that person's
      // ECDH public key NOW (the owner is by definition online at grant time),
      // store the wrapped key server-side, and hand over a link. The recipient
      // just signs in and accepts — the key is already waiting for them, so
      // there is no "request" and no "owner must come back online to approve".
      // The recoverable-key model means the recipient can re-derive their
      // private key on any device, so the wrap stays decryptable forever.
      const guestEmail = document.getElementById('share-target-email')?.value.trim() || '';
      if (!guestEmail) { throw new Error("Enter the person's email address — access is granted to a specific account"); }

      // Deny / allow enforcement (client-side, protects the owner from themselves).
      const sacCheck = checkShareAccessControl(guestEmail);
      if (!sacCheck.ok) throw new Error(sacCheck.reason);

      // The recipient must already have a STOCKROOM account so we can wrap the
      // key for their public key right now. (No deferred/pending-invite path.)
      const guestEmailHash = await kvHashEmail(guestEmail);
      const pubRes = await postKV(`${WORKER_URL}/user/ecdh-pubkey/get`, { emailHash: guestEmailHash });
      if (!pubRes.ok) {
        if (pubRes.status === 404) throw new Error(`${guestEmail} doesn't have a STOCKROOM account yet. Ask them to sign up first, then grant access.`);
        throw new Error('Could not fetch their encryption key — try again');
      }
      const { publicKeyJwk: guestPubKeyJwk } = await pubRes.json();

      // Owner's own ECDH keys (private to wrap with, public to send alongside).
      const ownerPrivKey = await loadEcdhPrivateKey(_kvEmailHash);
      if (!ownerPrivKey) throw new Error('Your encryption key is unavailable — sign out and back in, then retry');
      const ownerPubRes = await postKV(`${WORKER_URL}/user/ecdh-pubkey/get`, { emailHash: _kvEmailHash });
      if (!ownerPubRes.ok) throw new Error('Could not fetch your encryption key — try again');
      const { publicKeyJwk: ownerPubKeyJwk } = await ownerPubRes.json();

      // Generate the AES-GCM share key and wrap it for the guest.
      const shareKey    = await generateShareKey();
      const shareKeyB64 = await exportShareKey(shareKey);
      const wrappedKey  = await ecdhWrapShareKey(ownerPrivKey, guestPubKeyJwk, shareKey);

      // recordScoped: set when this create came from the bulk selection flow.
      const _isRecordScoped = !!(_bulkShareSelectionPending && Array.isArray(_bulkShareSelectionPending.ids) && _bulkShareSelectionPending.ids.length);

      // Create the share record. guestEmail is stored so the owner UI shows who
      // it's for; members starts with the granted guest so they're admitted on
      // first join with no approval step.
      const res = await postKV(`${WORKER_URL}/share/create`, {
          ownerEmailHash: _kvEmailHash,
          ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier },
          name, type: _shareTargetType, colour, ownerName,
          households: _shareTargetPerms,
          shareManagement: _shareTargetMgmt,
          guestEmail,
          grantedMember: guestEmailHash,
          ...(_isRecordScoped ? { recordScoped: true } : {}),
          householdNames: Object.fromEntries(
            Object.entries(profiles).map(([k,p]) => [k, p.name||(k==='default'?'Home':k)])
          ),
        });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Failed');

      // Store the wrapped key for the guest (server-side, persists until revoked).
      const ecdhRes = await postKV(`${WORKER_URL}/share/ecdh-key/store`, {
        ownerEmailHash: _kvEmailHash,
        ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier },
        code: data.code, guestEmailHash, wrappedKey, ownerPublicKeyJwk: ownerPubKeyJwk,
      });
      if (!ecdhRes.ok) throw new Error('Could not store the encrypted key for them — try again');

      // Cache share key locally and back it up (owner cross-device recovery).
      try {
        const stored = await _getShareKeys();
        stored[data.code] = shareKeyB64;
        await _setShareKeys(stored);
      } catch(e) {}
      await backupShareKey(data.code, shareKey).catch(e => console.warn('Share key backup failed:', e.message));

      // Push initial shared data.
      await pushSharedData(data.code, shareKey);

      // Enable household, close modal, show link.
      if (!_householdEnabled) {
        _householdEnabled = true;
        try { localStorage.setItem('stockroom_household', JSON.stringify({ enabled: true, colour: _householdColour })); } catch(e) {}
        connectPresence();
      }

      const inviteLink = data.link || `${location.origin}${location.pathname}?join=${data.code}`;
      try { await navigator.clipboard.writeText(inviteLink); } catch(e) {}

      // Email the invite link.
      if (WORKER_URL) {
        await _sendShareEmail(guestEmail, {
          code: data.code, name, type: _shareTargetType,
          households: _shareTargetPerms, isUpdate: false, inviteLink,
        }).catch(() => {});
      }

      await loadShareTargets();
      closeModal('share-target-modal');
      _shareTargetDone = false; // reset for next use

      // Bulk-share hand-off — apply allow overrides to selected records now
      // that we have a share code (fire-and-forget).
      try { await _applyBulkSharePending(data.code); } catch(e) { console.warn('_applyBulkSharePending failed:', e); }
      document.getElementById('bulk-share-pending-banner')?.remove();

      toast(`✓ ${name} granted access — link copied & emailed. They just sign in to accept.`);
      if (kvConnected) setTimeout(syncAll, 600);
    }
  } catch(err) {
    console.error('saveShareTarget:', err);
    toast('Could not save: ' + err.message);
    if (btn) { btn.textContent = code ? 'Save changes' : 'Create & get link'; btn.disabled = false; }
  }
}

async function refreshShareLink(code) {
  const target = _shareTargets.find(t => t.code === code);
  if (!target) return;
  try {
    const res = await postKV(`${WORKER_URL}/share/refresh`, { ownerEmailHash: _kvEmailHash, verifier: _kvVerifier, sessionToken: _kvSessionToken, code });
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
    const res = await postKV(`${WORKER_URL}/share/delete`, { ownerEmailHash: _kvEmailHash, verifier: _kvVerifier, sessionToken: _kvSessionToken, code });
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
      const res = await postKV(`${WORKER_URL}/share/delete`, { ownerEmailHash: _kvEmailHash, verifier: _kvVerifier, sessionToken: _kvSessionToken, code: target.code });
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
    await postKV(`${WORKER_URL}/share/send-email`, {
        ownerEmailHash: _kvEmailHash, ...authFields,
        guestEmail, code, name, type, households, isUpdate, inviteLink,
        ownerName: settings.email?.split('@')[0] || 'Your household',
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
