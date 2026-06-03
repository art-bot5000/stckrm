// ═══════════════════════════════════════════════════════════════════
//  NOTES UI (lazy-loaded half, extracted from notes.js)
// ═══════════════════════════════════════════════════════════════════
//
// This file is LAZY-LOADED — NOT referenced by a <script> tag in index.html.
// It is injected on demand by window._loadNotesUI() (defined in index.html,
// same once-only pattern as _loadScanner / _loadDemo / _loadBudgetUI) the
// first time the user opens the Notes view. For sessions where the user
// never opens Notes, ~1,300 lines stay out of the parse/execute path.
//
// It depends on notes.js (core) already being loaded: it freely reads/writes
// the core note state (notes, _noteUnlocked, _editingNoteId, _noteBodyDirty,
// _noteColourPickerOpen, _noteAutoSaveTimer, _NOTE_COLOUR_LIGHT_MAP) and
// calls core functions (loadNotes, saveNotes, _noteUid, _syncNoteIfConnected,
// _relockAllNotes, _closeNoteEditorImmediate, _startNoteInactivityTimer,
// _resetNoteActivity, _noteBgForCurrentTheme) by global name — exactly as it
// did when this was one file. Both are plain <script> (NOT modules) so all
// top-level declarations remain global.
//
// notes.js (core), notes-ui.js, app.js and index.html must all land in
// GitHub together.
// ═══════════════════════════════════════════════════════════════════

let _notesFilter = 'all';          // 'all'|'pinned'|'archived'|'trash'
let _noteUndoStack = new Map();    // noteId → string[]
let _noteRedoStack = new Map();    // noteId → string[]
let _noteOtpPending = false;       // waiting for 2FA OTP input

// ── Shared notes (Pass 3a) ────────────────────────────────────────────
// Notes received via shares from other people, grouped by share code so
// the UI can attribute them ("shared by Carla", etc). Each push from
// the owner REPLACES the bucket for that share code wholesale —
// reconcile-by-full-set means notes the owner stopped sharing simply
// vanish from the bucket on next sync, no tombstones needed.
//
// This bucket is intentionally separate from the user's own `notes[]`
// array. Merging shared-incoming notes into `notes` would cause the
// guest's next own-account push to re-publish them, creating loops and
// ownership confusion.
//
// Pass 3a: the bucket is filled by absorb but no UI reads it yet.
// Pass 3b will surface it as a "Shared with you" section in the Notes
// view. Pass 3c adds bulk-select on the user's personal notes.
let _sharedNotesIncoming = new Map();   // shareCode → Note[]

// ── Persistence (load/save) ──────────────────────────────────────
async function renderNotes() {
  await loadNotes();
  const grid  = document.getElementById('notes-grid');
  const empty = document.getElementById('notes-empty');
  if (!grid) return;

  // Show a heads-up banner for guests in shared households — their own
  // personal notes aren't household-shared (only the owner's notes that
  // they've individually shared with you appear in "Shared with you").
  // This banner explains why their personal notes don't sync to the
  // owner's account, distinct from groceries/reminders which DO sync
  // at section-perm level.
  let banner = document.getElementById('notes-share-info-banner');
  if (_shareState) {
    if (!banner) {
      banner = document.createElement('div');
      banner.id = 'notes-share-info-banner';
      banner.style.cssText = 'background:rgba(232,168,56,0.08);border:1px solid rgba(232,168,56,0.3);border-radius:10px;padding:10px 12px;margin-bottom:12px;font-size:12px;color:var(--text);display:flex;gap:8px;align-items:flex-start';
      banner.innerHTML = '<svg class="icon" aria-hidden="true" style="color:var(--accent);flex-shrink:0;margin-top:1px"><use href="#i-info"></use></svg><div><strong>Your notes are personal.</strong><br><span style="color:var(--muted)">Notes here are only on your account. The owner can share individual notes with you — those appear under <em>Shared with you</em>.</span></div>';
      const header = document.getElementById('notes-header');
      if (header && header.parentNode) {
        header.parentNode.insertBefore(banner, header.nextSibling);
      } else {
        grid.parentNode.insertBefore(banner, grid);
      }
    }
  } else if (banner) {
    banner.remove();
  }

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

  // Previously: applied a text-search filter on top of the chip filter
  // when the inline #notes-search input was present. That input was
  // removed when the global search took over — searching notes goes
  // through the global search modal instead.

  // Determine if there are shared notes for the current filter that
  // would render even if personal notes are empty. Shared notes only
  // appear in the 'all' filter (they don't participate in pinned/
  // archived/trash concepts from the recipient's perspective).
  const hasSharedToRender = _notesFilter === 'all'
    && _sharedNotesIncoming
    && _sharedNotesIncoming.size > 0
    && Array.from(_sharedNotesIncoming.values()).some(arr => Array.isArray(arr) && arr.length > 0);

  if (!visible.length && !hasSharedToRender) {
    grid.innerHTML = '';
    if (empty) empty.style.display = 'block';
    return;
  }
  if (empty) empty.style.display = 'none';

  // Sort: pinned first, then secured (locked notes), then everything else
  // Within each tier, most-recently-updated first.
  function _noteTier(n) {
    if (n.pinned) return 0;
    if (n.locked) return 1;
    return 2;
  }
  visible.sort((a, b) => {
    const ta = _noteTier(a), tb = _noteTier(b);
    if (ta !== tb) return ta - tb;
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

  // Empty trash pill — show only when viewing Trash and trash is non-empty
  const emptyChip = document.getElementById('notes-empty-trash-chip');
  if (emptyChip) {
    emptyChip.style.display = (_notesFilter === 'trash' && visible.length) ? '' : 'none';
  }

  // Three-tier section headers: Pinned, Secured, Other notes.
  // Headers only show when the 'all' filter is active and at least 2 of the
  // tiers have content (otherwise a single tier doesn't need a label).
  const hasPinned  = visible.some(n => n.pinned);
  const hasSecured = visible.some(n => !n.pinned && n.locked);
  const hasOther   = visible.some(n => !n.pinned && !n.locked);
  const tiersInUse = (hasPinned ? 1 : 0) + (hasSecured ? 1 : 0) + (hasOther ? 1 : 0);
  const showHeaders = tiersInUse >= 2 && _notesFilter === 'all';

  let html = '';
  let lastTier = -1;

  function _tierHeader(tier) {
    if (tier === 0) return `<div class="notes-section-label" style="padding:8px 0 4px"><svg class="icon" aria-hidden="true"><use href="#i-pin"></use></svg> Pinned</div>`;
    if (tier === 1) return `<div class="notes-section-label" style="padding:8px 0 4px"><svg class="icon" aria-hidden="true"><use href="#i-lock"></use></svg> Secure Notes</div>`;
    return `<div class="notes-section-label" style="padding:8px 0 4px"><svg class="icon" aria-hidden="true" style="vertical-align:-3px"><use href="#i-notebook-pen"></use></svg> Notes</div>`;
  }

  // ── "Shared with you" section (Pass 3b) ──
  // Renders ABOVE personal notes when _sharedNotesIncoming has entries
  // from any share. Only visible on the 'all' filter (pinned/archived/
  // trash filters apply to personal notes only — shared notes don't
  // participate in those concepts from the recipient's perspective).
  // Each card opens the read-only viewer modal on tap.
  if (_notesFilter === 'all' && _sharedNotesIncoming && _sharedNotesIncoming.size > 0) {
    let sharedHtml = '';
    let totalShared = 0;
    for (const [shareCode, sharedArr] of _sharedNotesIncoming.entries()) {
      if (!Array.isArray(sharedArr) || !sharedArr.length) continue;
      // Sharer name comes from _shareState.ownerName when we're a guest
      // of this share. Currently a guest can only have one share open
      // at a time, so this is reliable. Multi-share guests (future)
      // would need a share-code → name map.
      const sharerName = (_shareState && _shareState.code === shareCode)
        ? (_shareState.ownerName || 'a household member')
        : 'someone';
      sharedArr.forEach(n => {
        sharedHtml += _sharedNoteCardHTML(n, shareCode, sharerName);
        totalShared++;
      });
    }
    if (totalShared > 0) {
      html += `<div class="notes-section-label" style="padding:8px 0 4px"><svg class="icon" aria-hidden="true"><use href="#i-share-2"></use></svg> Shared with you</div>`;
      html += sharedHtml;
    }
  }

  visible.forEach(n => {
    const tier = _noteTier(n);
    if (showHeaders && tier !== lastTier) {
      html += _tierHeader(tier);
      lastTier = tier;
    }
    html += _noteCardHTML(n);
  });

  grid.innerHTML = html;
}

// Card for a note shared FROM someone else, rendered in the "Shared
// with you" section. Distinct from _noteCardHTML — no pin/archive/
// delete actions (the recipient doesn't own this note), no selection
// for bulk-select (Pass 3c excludes shared notes from bulk operations).
// Tap opens the read-only viewer modal.
function _sharedNoteCardHTML(n, shareCode, sharerName) {
  const bgStyle    = n.colour ? `background:${n.colour};` : '';
  const rawPreview = n.body || '';
  const _tmpDiv = document.createElement('div'); _tmpDiv.innerHTML = rawPreview;
  const previewText = (_tmpDiv.innerText || _tmpDiv.textContent || '').trim();
  const preview = previewText.slice(0, 120) + (previewText.length > 120 ? '…' : '');
  return `<div class="note-row" style="${bgStyle}" onclick="openSharedNoteViewer('${esc(shareCode)}','${esc(n.id)}')">
    <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:8px;margin-bottom:6px">
      <div style="flex:1;min-width:0">
        <div style="font-size:14px;font-weight:600;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(n.title || 'Untitled')}</div>
        <div style="font-size:10px;color:var(--muted);font-family:var(--mono);margin-top:2px"><svg class="icon icon-sm" aria-hidden="true" style="vertical-align:-2px"><use href="#i-share-2"></use></svg> shared by ${esc(sharerName)}</div>
      </div>
    </div>
    ${preview ? `<div style="font-size:12px;color:var(--muted);line-height:1.5;white-space:pre-wrap;word-break:break-word">${esc(preview)}</div>` : ''}
  </div>`;
}

// Open the shared-note viewer for a specific note. Read-only — body is
// rendered as innerHTML but the container is not contenteditable. No
// autosave, no toolbar, no editing actions. Tap the back button to
// return to the notes list.
function openSharedNoteViewer(shareCode, noteId) {
  const arr = _sharedNotesIncoming?.get(shareCode);
  const n = Array.isArray(arr) ? arr.find(x => x.id === noteId) : null;
  if (!n) { console.warn('openSharedNoteViewer: note not found', shareCode, noteId); return; }
  const overlay   = document.getElementById('shared-note-viewer-overlay');
  const titleEl   = document.getElementById('shared-note-viewer-title');
  const attribEl  = document.getElementById('shared-note-viewer-attribution');
  const bodyEl    = document.getElementById('shared-note-viewer-body');
  if (!overlay || !titleEl || !bodyEl) return;
  const sharerName = (_shareState && _shareState.code === shareCode)
    ? (_shareState.ownerName || 'a household member')
    : 'someone';
  titleEl.textContent  = n.title || 'Untitled';
  // Attribution shows sharer + perm (r vs rw, from the per-note effective
  // perm annotated by the owner's filter). For Pass 3b, rw is not yet
  // editor-enabled — display is informational only.
  const permLabel = n._shareEffectivePerm === 'rw' ? 'read & write' : 'read-only';
  attribEl.innerHTML = `<svg class="icon icon-sm" aria-hidden="true"><use href="#i-share-2"></use></svg> shared by ${esc(sharerName)} · ${esc(permLabel)}`;
  // Body. innerHTML because notes are stored as HTML (rich text in the
  // editor). Container is NOT contenteditable; viewer is strictly read.
  bodyEl.innerHTML = n.body || '';
  bodyEl.style.background = n.colour || 'transparent';
  overlay.style.display = 'flex';
  document.body.classList.add('note-open');
}

function closeSharedNoteViewer() {
  const overlay = document.getElementById('shared-note-viewer-overlay');
  if (overlay) overlay.style.display = 'none';
  document.body.classList.remove('note-open');
  // Clear body so we don't keep stale HTML in memory (or visible on
  // a flash if the viewer reopens quickly with a different note).
  const bodyEl = document.getElementById('shared-note-viewer-body');
  if (bodyEl) bodyEl.innerHTML = '';
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
  if (n.pinned)  icons.push('<svg class="icon" aria-hidden="true"><use href="#i-pin"></use></svg>');
  if (n.locked)  icons.push(isUnlocked ? '<svg class="icon" aria-hidden="true"><use href="#i-unlock"></use></svg>' : '<svg class="icon" aria-hidden="true"><use href="#i-lock"></use></svg>');
  if (n.archived) icons.push('<svg class="icon" aria-hidden="true"><use href="#i-archive"></use></svg>');
  // Owner-only share indicator. Visible when the note has an explicit
  // share state. Hidden for guests viewing their own (always-private)
  // notes, and for owners' notes that haven't been shared yet (no
  // share field). Matches the indicator pattern used on stockroom /
  // grocery / reminder / category cards.
  if (isOwner() && n.share != null) {
    icons.push('<svg class="icon" aria-hidden="true" style="color:var(--accent)" title="Shared"><use href="#i-share-2"></use></svg>');
  }
  if (n.deletedAt) {
    const daysLeft = Math.max(0, 30 - Math.round((Date.now()-new Date(n.deletedAt).getTime())/MS_PER_DAY));
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
      await postKV(`${WORKER_URL}/note/body/delete`, { emailHash:_kvEmailHash, ..._kvSessionToken?{sessionToken:_kvSessionToken}:{verifier:_kvVerifier}, noteId:id }).catch(()=>{});
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
  const res = await postKV(`${WORKER_URL}/note/body/push`, { emailHash:_kvEmailHash, ..._kvSessionToken?{sessionToken:_kvSessionToken}:{verifier:_kvVerifier}, noteId, ciphertext });
  if (res.status === 402 && window.stockroomBilling) {
    try { await stockroomBilling.handleApiError(res); } catch (_) {}
    return;
  }
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
  // No-op shim — the inline notes search input was removed when the global
  // search took over. Kept as a stub in case anything external still calls
  // it; safe to delete once we're certain nothing does.
  // Intentionally empty.
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
    // Set body.note-open so the CSS rules in styles.css (lines ~1467-1477)
    // can hide the FAB and lock background scroll. Without this class the
    // FAB stays at z-index 1100 and renders on top of the note editor.
    document.body.classList.add('note-open');
    show('note-editor-body', 'flex');
    hide('note-lock-screen');
    _renderNoteEditor(n, false);
    _showNoteBody(n); // always set body (clears previous note's content from contenteditable)
    // Brand-new note: hide the sharing panel until the user titles it.
    // (Sharing an untitled note from the editor would be confusing; the
    // user can save first via title-input, then re-open to share.)
    const sharingEl = document.getElementById('note-sharing-section');
    if (sharingEl) sharingEl.style.display = 'none';
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
  // Match the new-note branch above — set body.note-open so CSS hides the
  // FAB and locks background scroll while the editor is up.
  document.body.classList.add('note-open');
  _renderNoteEditor(n, n.locked && !isUnlocked);
  // Show/hide the per-note sharing panel based on note state. Suppressed
  // for locked, trashed, and untitled notes.
  _showNoteSharingPanel(n);

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
  const secureBadgeLabel = document.getElementById('note-secure-badge-label');
  if (secureBadge) {
    if (!n.locked) {
      secureBadge.style.display = 'none';
    } else {
      // The badge is a flex button; use inline-flex so the icon+label align.
      secureBadge.style.display = 'inline-flex';
      const isOpen = _noteUnlocked.has(n.id);
      if (secureBadgeLabel) {
        secureBadgeLabel.textContent = isOpen ? 'OPEN — TAP TO LOCK' : 'SECURE NOTE';
      }
      secureBadge.title = isOpen ? 'Click to lock now' : 'This note is locked';
      secureBadge.style.cursor = isOpen ? 'pointer' : 'default';
    }
  }

  // Colour swatches
  document.querySelectorAll('.note-swatch').forEach(s => {
    s.classList.toggle('active', (s.dataset.colour || '') === (n.colour || ''));
  });

  // Editor background
  const overlay = document.getElementById('note-editor-overlay');
  if (overlay) overlay.style.background = _noteBgForCurrentTheme(n.colour);

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

// ── Per-note sharing-panel registration (Pass 3b) ──────────────────────
// Notes don't have a section perm — each note's `share` field is the
// sole gate. defaultBehavior: 'private' tells the generic sharing-panel
// module that absent `share` means owner-only (instead of inherit).
//
// Locked notes can't be shared (recipients don't have the password);
// the panel is suppressed for them in _showNoteSharingPanel() below.
registerSharingSection('note', {
  findRecord: (id) => notes.find(n => n.id === id),
  currentId:  ()   => _editingNoteId,
  save:       ()   => saveNotes(),
  mountSectionEl: () => document.getElementById('note-sharing-section'),
  mountContentEl: () => document.getElementById('note-sharing-content'),
  noun: 'note',
  defaultBehavior: 'private',  // no section perm to inherit from
});

// Decide whether to show the per-note sharing panel for the currently
// open note. Hidden for: new (untitled) notes, locked notes, and trashed
// notes. Hidden via spec.mountSectionEl. Called after _renderNoteEditor
// from openNoteEditor.
function _showNoteSharingPanel(n) {
  const sectionEl = document.getElementById('note-sharing-section');
  if (!sectionEl) return;
  // New unsaved notes (no title yet) — skip; user should title it first
  // before deciding to share. Avoids early "share my empty note" UX.
  // Locked notes — never shared.
  // Trashed notes — can't share something thrown away.
  if (!n || !n.title || n.locked || n.deletedAt) {
    sectionEl.style.display = 'none';
    return;
  }
  openSharingPanelFor('note');  // module renders the summary
}

function _showNoteLockScreen(n) {
  hide('note-editor-body');
  show('note-lock-screen', 'flex');
  document.getElementById('note-lock-title').textContent = n.title;
  document.getElementById('note-lock-error').textContent = '';
  hide('note-otp-section');
  show('note-unlock-btn', 'block');
  _noteOtpPending = false;
}

function _showNoteBody(n) {
  hide('note-lock-screen');
  const editorBody = document.getElementById('note-editor-body');
  if (editorBody) editorBody.style.display = 'flex';

  const unlocked = _noteUnlocked.get(n.id);
  const body = unlocked ? unlocked.body : (n.body || '');

  if (n.tickBoxesVisible) {
    // If tickItems isn't yet populated (legacy notes saved before this refactor),
    // build it from the current body so the renderer has structured data.
    if (!n.tickItems || !n.tickItems.length) {
      const lines = _flattenContentEditableToLines(body || '');
      n.tickItems = lines.map((text, i) => ({
        text,
        checked: !!(n.tickBoxes||{})[i],
        originalIndex: i,
      }));
    }
    _renderTickBody(n);
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
    const res = await postKV(`${WORKER_URL}/note/otp/send`, {
        emailHash: _kvEmailHash,
        ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier },
      });
    if (!res.ok) {
      const d = await _readJsonSafe(res) || {};
      errEl.textContent = d.error || `Could not send code (${res.status})`;
      return;
    }
    // Show OTP input
    hide('note-unlock-btn');
    show('note-otp-section', 'flex');
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
    const res = await postKV(`${WORKER_URL}/note/otp/verify`, { emailHash: _kvEmailHash, otp });
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
    if (!_kvSessionToken && !_kvVerifier) {
      errEl.textContent = 'Not signed in — please refresh and try again';
      return;
    }
    const res = await postKV(`${WORKER_URL}/note/body/pull`, {
        emailHash: _kvEmailHash,
        ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier },
        noteId: n.id,
      });
    const data = await _readJsonSafe(res);
    if (!res.ok) {
      if (res.status === 404) {
        // The note's encrypted body is missing from the server. This can happen
        // if the note was locked but the body upload failed silently, or if
        // the note metadata synced from another device but the body never did.
        // Offer to remove security so the user can re-enter content.
        errEl.innerHTML = `Note body is missing from server.<br><br>This can happen if security was enabled but the encrypted content was never uploaded. <a href="#" onclick="_removeNoteSecurity('${n.id}'); return false;" style="color:var(--accent);text-decoration:underline">Remove security from this note</a> so you can re-add the content.`;
        return;
      }
      // Common cause: empty body from a proxy error or auth middleware
      const msg = data?.error || data?._raw || `Server returned ${res.status}`;
      errEl.textContent = msg;
      return;
    }
    if (!data || !data.ciphertext) {
      errEl.textContent = 'Server returned an empty response — try again';
      return;
    }
    if (!await kvEnsureKey()) { errEl.textContent = 'Encryption key unavailable'; return; }
    const body = await kvDecrypt(_kvKey, data.ciphertext);
    // Cache in memory (body stored as innerHTML)
    _noteUnlocked.set(n.id, { body, lastActivity: Date.now(), inactivityTimer: null });
    _startNoteInactivityTimer(n.id);
    _showNoteBody(n);
    _renderNoteEditor(n, false);
    // Advise the user how long the note will stay open + offer immediate re-lock.
    setTimeout(() => {
      toastAction('This note will stay open for 30 minutes', {
        label: 'Secure now',
        onclick: () => secureLockNote(n.id),
      });
    }, 200);
  } catch(e) {
    errEl.textContent = 'Could not unlock: ' + (e.message || 'unknown error');
  }
}

// Recovery: remove security from a note when the encrypted body is gone from
// the server (e.g. push silently failed). Clears the locked flag locally so
// the user can edit the note normally and add fresh content.
// Click handler for the floating "SECURE NOTE" badge in the editor.
// If the note is currently unlocked (plaintext in memory), lock it immediately.
// Otherwise show a brief explanatory toast.
async function _badgeLockNow() {
  const id = _editingNoteId; if (!id) return;
  const n = notes.find(x => x.id === id); if (!n) return;
  if (n.locked && _noteUnlocked.has(id)) {
    // Auto-save first so the latest edits are encrypted
    if (_noteBodyDirty) {
      clearTimeout(_noteAutoSaveTimer);
      await _autoSaveNote();
    }
    await secureLockNote(id);
  } else if (n.locked) {
    toast('This note is already locked');
  } else {
    toast('Use the lock button in the toolbar to secure this note');
  }
}

async function _removeNoteSecurity(noteId) {
  const n = notes.find(x => x.id === noteId); if (!n) return;
  if (!confirm('Remove security from this note?\n\nThe note will become editable but its previous content cannot be recovered.')) return;
  n.locked = false;
  n.body = ''; // empty body so user can type fresh content
  n.updatedAt = new Date().toISOString();
  _noteUnlocked.delete(noteId);
  // Best-effort delete the (probably non-existent) server body so storage is clean
  await postKV(`${WORKER_URL}/note/body/delete`, {
      emailHash: _kvEmailHash,
      ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier },
      noteId,
    }).catch(() => {});
  await saveNotes();
  await _syncNoteIfConnected();
  // Reload editor to show empty editable body
  _showNoteBody(n);
  _renderNoteEditor(n, false);
  toast('Security removed — you can now edit this note');
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
    await postKV(`${WORKER_URL}/note/body/delete`, {
        emailHash: _kvEmailHash,
        ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier },
        noteId: n.id,
      }).catch(() => {});
    toast('Note is no longer secured');
  } else {
    // Locking — push body to server and strip from local
    if (!n.body && !_noteUnlocked.get(n.id)?.body) { toast('Add some content first'); return; }
    const body = _noteUnlocked.get(n.id)?.body || n.body || '';
    // Pre-flight: secure note bodies require sync credentials. Without these,
    // the body cannot be uploaded to the server — bail with a clear message
    // rather than letting the request fail mysteriously.
    if (!_kvEmailHash) {
      toast('Sign in to enable secure notes');
      console.warn('[note lock] _kvEmailHash is empty — user not fully authenticated');
      return;
    }
    if (!_kvSessionToken && !_kvVerifier) {
      toast('Session expired — please refresh and sign in again');
      console.warn('[note lock] no sessionToken or verifier available');
      return;
    }
    if (!await kvEnsureKey()) {
      toast('Encryption key unavailable — try refreshing');
      return;
    }
    let ciphertext;
    try {
      ciphertext = await kvEncrypt(_kvKey, body);
    } catch (e) {
      toast('Could not encrypt note: ' + (e.message || 'unknown error'));
      console.error('[note lock] encrypt error:', e);
      return;
    }
    let res, errText = '';
    try {
      res = await postKV(`${WORKER_URL}/note/body/push`, {
          emailHash: _kvEmailHash,
          ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier },
          noteId: n.id, ciphertext,
        });
    } catch (e) {
      toast('Could not reach server: ' + (e.message || 'network error'));
      console.error('[note lock] fetch error:', e);
      return;
    }
    if (res.status === 402 && window.stockroomBilling) {
      try { await stockroomBilling.handleApiError(res); } catch (_) {}
      return;
    }
    if (!res.ok) {
      // Try to parse a JSON error body so the user sees the real reason
      try {
        const txt = await res.text();
        if (txt && txt.trim()) {
          try {
            const d = JSON.parse(txt);
            errText = d.error || txt.slice(0, 100);
          } catch(_) { errText = txt.slice(0, 100); }
        }
      } catch(_) {}
      const reason = errText
        ? `${res.status}: ${errText}`
        : `Server returned ${res.status}`;
      toast('Could not secure note — ' + reason);
      console.error('[note lock] push failed:', res.status, errText);
      return;
    }
    n.body   = undefined;
    n.locked = true;
    _noteUnlocked.set(n.id, { body, lastActivity: Date.now(), inactivityTimer: null });
    _startNoteInactivityTimer(n.id);
    setTimeout(() => {
      toastAction('Note secured. It will stay open here for 30 minutes', {
        label: 'Secure now',
        onclick: () => secureLockNote(n.id),
      });
    }, 100);
  }
  n.updatedAt = new Date().toISOString();
  _renderNoteEditor(n, false);
  await saveNotes(); await _syncNoteIfConnected();
}

// Flatten contenteditable HTML into an array of plain-text lines.
// Browsers wrap pressed-Enter lines differently — Chrome wraps subsequent
// lines in <div>, Firefox uses <br>. The naive
// `replaceWith('\n')` + `insertAdjacentText('afterend', '\n')` approach
// merges line 1 with line 2 when the first line is bare text without a wrapper:
//     "foo<div>bar</div><div>baz</div>" → "foo" + "bar\n" + "baz\n"
//                                       → "foobar\nbaz" ❌
// This walker emits text + newlines for each block-level node correctly so
// every hit-Enter line becomes its own item.
function _flattenContentEditableToLines(html) {
  const tmp = document.createElement('div');
  tmp.innerHTML = html || '';
  const BLOCKS = new Set(['P', 'DIV', 'LI', 'BLOCKQUOTE', 'PRE', 'H1', 'H2', 'H3', 'H4', 'H5', 'H6']);
  let out = '';
  function walk(node) {
    for (const child of node.childNodes) {
      if (child.nodeType === Node.TEXT_NODE) {
        out += child.textContent;
      } else if (child.nodeType === Node.ELEMENT_NODE) {
        const tag = child.tagName;
        if (tag === 'BR') {
          out += '\n';
        } else if (BLOCKS.has(tag)) {
          // Ensure block boundaries are preserved on both sides
          if (out && !out.endsWith('\n')) out += '\n';
          walk(child);
          if (!out.endsWith('\n')) out += '\n';
        } else {
          walk(child);
        }
      }
    }
  }
  walk(tmp);
  return out.split('\n').map(l => l.trim()).filter(Boolean);
}

async function toggleNoteTicks() {
  const n = notes.find(x => x.id === _editingNoteId); if (!n) return;
  n.tickBoxesVisible = !n.tickBoxesVisible;
  if (!n.tickBoxes) n.tickBoxes = {};
  document.getElementById('note-btn-tick')?.classList.toggle('active', n.tickBoxesVisible);
  const bodyEl  = document.getElementById('note-body-input');
  const ticksEl = document.getElementById('note-ticks-body');

  if (n.tickBoxesVisible) {
    // Switching ON — convert current rich-text body to ordered tickItems
    const rawHTML = bodyEl ? bodyEl.innerHTML : (n.body || '');
    const lines = _flattenContentEditableToLines(rawHTML);
    // Preserve previously-checked state if tickItems already exists; otherwise
    // fall back to the legacy n.tickBoxes index map.
    const prevByText = new Map((n.tickItems || []).map(it => [it.text, !!it.checked]));
    n.tickItems = lines.map((text, i) => ({
      text,
      checked: prevByText.has(text) ? prevByText.get(text) : !!(n.tickBoxes||{})[i],
      originalIndex: i,
    }));
    if (bodyEl) bodyEl.style.display = 'none';
    _renderTickBody(n);
  } else {
    // Switching OFF — reconstruct body in ORIGINAL order (not display order)
    const items = (n.tickItems && n.tickItems.length)
      ? [...n.tickItems].sort((a,b) => a.originalIndex - b.originalIndex)
      : null;
    if (bodyEl) {
      bodyEl.style.display = '';
      if (items) {
        // Wrap each line in a <div> so contenteditable preserves line breaks
        // reliably (the prior <br>-only approach merged the first two lines
        // in some browsers).
        bodyEl.innerHTML = items.map(it => `<div>${esc(it.text) || '<br>'}</div>`).join('');
      } else if (ticksEl) {
        // Fallback when there are no tickItems yet (legacy notes)
        const labels = ticksEl.querySelectorAll('label span');
        const lines  = [...labels].map(s => s.textContent);
        bodyEl.innerHTML = lines.map(l => `<div>${esc(l) || '<br>'}</div>`).join('');
      }
    }
    if (ticksEl) ticksEl.style.display = 'none';
  }
  n.updatedAt = new Date().toISOString();
  await saveNotes();
}

function _renderTickBody(n, _legacyBody) {
  const container = document.getElementById('note-ticks-body');
  if (!container) return;
  container.style.display = 'block';

  // Build tickItems from legacy body argument if not yet populated (back-compat)
  if ((!n.tickItems || !n.tickItems.length) && typeof _legacyBody === 'string') {
    const lines = _flattenContentEditableToLines(_legacyBody);
    n.tickItems = lines.map((text, i) => ({
      text,
      checked: !!(n.tickBoxes||{})[i],
      originalIndex: i,
    }));
  }

  const items = n.tickItems || [];
  if (!items.length) {
    container.innerHTML = `<p style="color:var(--muted);font-size:13px">Add some lines — each line becomes a tick box.</p>`;
    return;
  }

  // Display order: unchecked first (original order), then checked (original order)
  const sorted = [...items.entries()]
    .sort(([, a], [, b]) => {
      if (a.checked !== b.checked) return a.checked ? 1 : -1;
      return (a.originalIndex || 0) - (b.originalIndex || 0);
    });

  // Layout note: NO <label> wrapper — wrapping in <label> means clicking the
  // text bubbles to the checkbox and toggles it, making the text uneditable.
  // We separate the checkbox into its own clickable area and put the text in
  // a contenteditable span that captures clicks for focus/edit.
  container.innerHTML = sorted.map(([realIdx, it]) => {
    const lineStyle = it.checked
      ? 'text-decoration:line-through;color:var(--muted)'
      : 'color:var(--text)';
    return `<div class="tick-row" style="display:flex;align-items:flex-start;gap:10px;padding:8px 0;border-bottom:1px solid var(--border)">
      <input type="checkbox" ${it.checked ? 'checked' : ''} onchange="onNoteTick(${realIdx},this.checked)"
        style="margin-top:3px;width:18px;height:18px;min-width:18px;accent-color:var(--accent);cursor:pointer;flex-shrink:0">
      <span class="tick-text" contenteditable="true" data-tickidx="${realIdx}"
        oninput="onTickTextEdit(${realIdx}, this.textContent)"
        onkeydown="return onTickTextKeydown(event, ${realIdx})"
        style="flex:1;${lineStyle};font-size:14px;line-height:1.5;outline:none;cursor:text;min-height:21px">${esc(it.text)}</span>
    </div>`;
  }).join('');
}

async function onNoteTick(idx, checked) {
  const n = notes.find(x => x.id === _editingNoteId); if (!n) return;
  if (!n.tickBoxes) n.tickBoxes = {};
  n.tickBoxes[idx] = checked;
  if (n.tickItems && n.tickItems[idx]) {
    n.tickItems[idx].checked = checked;
  }
  n.updatedAt = new Date().toISOString();
  // Re-render so strikethrough/grey applies and checked items move to bottom
  _renderTickBody(n);
  await saveNotes();
  if (n.locked) _resetNoteActivity(n.id);
}

// Inline edit of a tick item's text. Updates the tickItem and triggers the
// debounced autosave but does NOT re-render (would lose caret position).
function onTickTextEdit(idx, text) {
  const n = notes.find(x => x.id === _editingNoteId); if (!n) return;
  if (!n.tickItems || !n.tickItems[idx]) return;
  n.tickItems[idx].text = text;
  _noteBodyDirty = true;
  clearTimeout(_noteAutoSaveTimer);
  _noteAutoSaveTimer = setTimeout(_autoSaveNote, 1200);
  if (n.locked) _resetNoteActivity(n.id);
}

// Handle Enter to add a new line below, Backspace on empty to merge with prev.
function onTickTextKeydown(event, idx) {
  const n = notes.find(x => x.id === _editingNoteId); if (!n) return true;
  if (!n.tickItems) return true;

  if (event.key === 'Enter' && !event.shiftKey) {
    event.preventDefault();
    // Insert a new empty tickItem right after this one in original order
    const it = n.tickItems[idx];
    const newOriginal = (it.originalIndex || 0) + 0.5; // fractional → re-stride later
    n.tickItems.push({ text: '', checked: false, originalIndex: newOriginal });
    // Re-stride originalIndex to integers
    [...n.tickItems]
      .sort((a, b) => (a.originalIndex || 0) - (b.originalIndex || 0))
      .forEach((item, i) => { item.originalIndex = i; });
    _renderTickBody(n);
    // Focus the new line
    setTimeout(() => {
      const newIdx = n.tickItems.findIndex(t => t.originalIndex === Math.ceil(newOriginal));
      const span = document.querySelector(`.tick-text[data-tickidx="${newIdx}"]`);
      if (span) {
        span.focus();
        // Place caret at end (which is start since text is empty)
        const range = document.createRange();
        range.selectNodeContents(span);
        range.collapse(false);
        const sel = window.getSelection();
        sel.removeAllRanges();
        sel.addRange(range);
      }
    }, 0);
    _noteBodyDirty = true;
    clearTimeout(_noteAutoSaveTimer);
    _noteAutoSaveTimer = setTimeout(_autoSaveNote, 1200);
    return false;
  }

  if (event.key === 'Backspace') {
    const span = event.target;
    if (span && span.textContent === '' && n.tickItems.length > 1) {
      event.preventDefault();
      // Remove this item, focus the previous in original order
      const removedOriginal = n.tickItems[idx].originalIndex;
      n.tickItems.splice(idx, 1);
      // Re-stride
      [...n.tickItems]
        .sort((a, b) => (a.originalIndex || 0) - (b.originalIndex || 0))
        .forEach((item, i) => { item.originalIndex = i; });
      _renderTickBody(n);
      setTimeout(() => {
        // Focus the item that was directly above (one less originalIndex)
        const prevTarget = Math.max(0, removedOriginal - 1);
        const prevIdx = n.tickItems.findIndex(t => t.originalIndex === prevTarget);
        const prev = document.querySelector(`.tick-text[data-tickidx="${prevIdx}"]`);
        if (prev) {
          prev.focus();
          const range = document.createRange();
          range.selectNodeContents(prev);
          range.collapse(false);
          const sel = window.getSelection();
          sel.removeAllRanges();
          sel.addRange(range);
        }
      }, 0);
      _noteBodyDirty = true;
      clearTimeout(_noteAutoSaveTimer);
      _noteAutoSaveTimer = setTimeout(_autoSaveNote, 1200);
      return false;
    }
  }

  return true;
}

function toggleNoteColourPicker() {
  const picker = document.getElementById('note-colour-picker');
  _noteColourPickerOpen = !_noteColourPickerOpen;
  picker.style.display = _noteColourPickerOpen ? 'flex' : 'none';
}

async function setNoteColour(colour) {
  const n = notes.find(x => x.id === _editingNoteId); if (!n) return;
  n.colour = colour || null; n.updatedAt = new Date().toISOString();
  document.getElementById('note-editor-overlay').style.background = _noteBgForCurrentTheme(colour);
  document.querySelectorAll('.note-swatch').forEach(s =>
    s.classList.toggle('active', (s.dataset.colour || '') === (colour || ''))
  );
  _noteColourPickerOpen = false;
  hide('note-colour-picker');
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
      await postKV(`${WORKER_URL}/note/body/delete`, { emailHash: _kvEmailHash, ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier }, noteId: n.id }).catch(() => {});
    }
  } else {
    if (!confirm(`Move "${n.title}" to trash?\n\nYou can restore it within 30 days from the Trash filter.`)) return;
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
    // Source of truth in tick mode is n.tickItems (original order) so saves
    // don't depend on the visual sort that puts checked items at the bottom.
    if (n.tickItems && n.tickItems.length) {
      return [...n.tickItems]
        .sort((a,b) => (a.originalIndex||0) - (b.originalIndex||0))
        .map(it => it.text)
        .join('\n');
    }
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
  // Update the title on the record so the sharing-panel visibility
  // check (which gates on title being non-empty) sees the latest state.
  // Without this, the panel stays hidden after the user types a title
  // because _showNoteSharingPanel was only called once at editor open.
  if (n) {
    const titleEl = document.getElementById('note-title-input');
    if (titleEl) n.title = titleEl.value;
    _showNoteSharingPanel(n);
  }
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
      await postKV(`${WORKER_URL}/note/body/push`, {
          emailHash: _kvEmailHash,
          ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier },
          noteId: n.id, ciphertext,
        }).then(async (res) => {
        if (res && res.status === 402 && window.stockroomBilling) {
          try { await stockroomBilling.handleApiError(res); } catch (_) {}
        }
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
