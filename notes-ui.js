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
// _sharedNotesIncoming is declared eagerly in app.js (see the note there) so
// that kvSyncNow can absorb shared notes before this lazy-loaded file exists.
// Reuse that same global rather than re-declaring — a fresh `let` here would
// shadow/reset the map and discard notes absorbed before the Notes view was
// first opened. Only initialise if app.js somehow hasn't (defensive).
if (typeof _sharedNotesIncoming === 'undefined' || !(_sharedNotesIncoming instanceof Map)) {
  window._sharedNotesIncoming = new Map();
}
// shareCode → Note[]  (lives on window, set up by app.js)

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
  // Draw notes carry vector strokes; render them as inline SVG rather than
  // dumping the raw payload as HTML.
  // Draw notes: render the vector strokes as inline SVG (scales to any
  // size). Legacy PNG notes (old engine) fall back to the stored image.
  if (n.drawMode) {
    const vd = _noteVectorData(n);
    if (vd.strokes.length || vd.photo) {
      bodyEl.innerHTML = `<div style="display:flex;justify-content:center">${_strokesToSvg(vd.strokes, vd.aspect, 760, 560, n.drawBg, vd.photo, vd.photoT, vd.photoT && vd.photoT.na)}</div>`;
    } else {
      const url = _noteLegacyPngUrl(n);
      bodyEl.innerHTML = url
        ? `<img src="${url}" alt="Drawing" style="max-width:100%;height:auto;border-radius:8px;background:var(--bg)">`
        : '<p style="color:var(--muted)">This note is a drawing.</p>';
    }
  } else {
    bodyEl.innerHTML = n.body || '';
  }
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
  // Draw notes carry a PNG, not text. For unlocked draw notes show a
  // thumbnail; for locked ones (drawing is encrypted) just the lock hint.
  const isDraw     = !!n.drawMode;
  // Card thumbnail: render vector strokes as inline SVG (resolution-free).
  // Locked & still-locked notes show only the lock hint. Legacy PNG notes
  // (old engine, no strokes) fall back to the stored image.
  let drawThumb = '';
  let drawThumbIsSvg = false;
  if (isDraw && !(n.locked && !isUnlocked)) {
    const vd = _noteVectorData(n);
    if (vd.strokes.length || vd.photo) {
      drawThumb = _strokesToSvg(vd.strokes, vd.aspect, 300, 160, n.drawBg, vd.photo, vd.photoT, vd.photoT && vd.photoT.na);
      drawThumbIsSvg = true;
    } else {
      drawThumb = _noteLegacyPngUrl(n);  // legacy PNG data-URL or ''
    }
  }
  const rawPreview = (isDraw || (n.locked && !isUnlocked)) ? '' : (unlocked?.body || n.body || '');
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
  if (isDraw) icons.push('<svg class="icon" aria-hidden="true" title="Drawing"><use href="#i-pencil"></use></svg>');
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
          : isDraw
            ? (drawThumb
                ? (drawThumbIsSvg
                    ? `<div class="note-row-drawing" style="margin-top:6px;max-height:120px;overflow:hidden;border-radius:6px;background:var(--bg);display:flex;justify-content:center">${drawThumb}</div>`
                    : `<img src="${drawThumb}" alt="Drawing" class="note-row-drawing" style="margin-top:6px;max-height:120px;max-width:100%;border-radius:6px;background:var(--bg);display:block">`)
                : `<div style="font-size:12px;color:var(--muted);margin-top:3px">✏️ Drawing</div>`)
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
      drawMode: false, drawStrokes: null, drawAspect: 1, drawBg: 'none',
      drawPhoto: null, drawPhotoT: null, drawing: null,
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
  document.getElementById('note-btn-draw')?.classList.toggle('active', !!n.drawMode);
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
  const drawWrap = document.getElementById('note-draw-wrap');

  if (n.drawMode) {
    // Drawing mode is mutually exclusive with text/tick bodies.
    const ta = document.getElementById('note-body-input');
    if (ta) ta.style.display = 'none';
    const ticksBody = document.getElementById('note-ticks-body');
    if (ticksBody) ticksBody.style.display = 'none';
    const fmt = document.getElementById('note-fmt-toolbar');
    if (fmt) fmt.style.display = 'none';
    if (drawWrap) drawWrap.style.display = 'flex';
    // Defer canvas init to next frame so the host has its final size
    // (the overlay/editor-body may have only just become visible).
    requestAnimationFrame(() => _initNoteDrawCanvas(n));
    return;
  }
  if (drawWrap) drawWrap.style.display = 'none';
  // Leaving draw mode (or never in it): restore the formatting toolbar.
  // Tick mode keeps the toolbar shown exactly as before this feature.
  const fmt = document.getElementById('note-fmt-toolbar');
  if (fmt) fmt.style.display = 'flex';

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
  // Commit any open inline text editor so typed text isn't lost on close.
  if (_noteDrawState && _noteDrawState._textEdit) _commitNoteTextEdit();
  if (_noteBodyDirty) await _autoSaveNote();
  _closeNoteEditorImmediate();
  // Discard untitled empty notes — don't litter the grid with blank cards
  if (id) {
    const n = notes.find(x => x.id === id);
    // A draw-mode note is "empty" only if it has no strokes AND no photo
    // (and no legacy PNG), regardless of the auto-applied "Drawing" title.
    const live = (_noteDrawState && _noteDrawState.noteId === id) ? _noteDrawState : null;
    const liveEmpty = live ? (live.strokes.length === 0 && !live.photoData) : null;
    const drawEmpty = n && n.drawMode
      && (liveEmpty !== null ? liveEmpty : (!_noteHasVector(n) && !_noteLegacyPngUrl(n)));
    if (n && n.drawMode) {
      if (drawEmpty) { notes = notes.filter(x => x.id !== id); await saveNotes(); }
    } else if (n && !n.title?.trim() && !n.body?.trim() && !(document.getElementById('note-body-input')?.innerHTML?.trim())) {
      notes = notes.filter(x => x.id !== id);
      await saveNotes();
    }
  }
  _noteDrawState = null;
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
    if (n.drawMode && (unlocked.body || '').startsWith('\u0001VDRAW\u0001')) {
      // Restore the plaintext vector strokes + photo so cards/preview render.
      try {
        const obj = JSON.parse(unlocked.body.slice('\u0001VDRAW\u0001'.length));
        n.drawStrokes = obj.s || [];
        n.drawAspect  = obj.a || 1;
        n.drawPhoto   = obj.p || null;
        n.drawPhotoT  = obj.pt || null;
      } catch (_) { n.drawStrokes = []; n.drawAspect = 1; n.drawPhoto = null; n.drawPhotoT = null; }
      n.drawing = undefined;
      n.body = '';
    } else if (n.drawMode && (unlocked.body || '').startsWith('\u0001DRAW\u0001')) {
      // Legacy locked PNG drawing — keep it as display-only artwork.
      n.drawing = unlocked.body.slice('\u0001DRAW\u0001'.length) || null;
      n.body = '';
    } else {
      n.body = unlocked.body; // stored as innerHTML
    }
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
    // For a draw note the encryptable payload is the drawing sentinel, not
    // n.body (which is empty in draw mode). Build it the same way the
    // editor save path does so locked drawings round-trip correctly.
    if (n.drawMode) {
      if (_noteBodyDirty) { try { await _autoSaveNote(); } catch (_) {} }
    }
    const drawBody = n.drawMode
      ? (_noteUnlocked.get(n.id)?.body
         || (_noteHasVector(n) ? _serializeVDraw(n.drawStrokes, n.drawAspect || 1, n.drawPhoto, n.drawPhotoT) : ''))
      : null;
    if (n.drawMode && !drawBody) { toast('Draw something first'); return; }
    if (!n.drawMode && !n.body && !_noteUnlocked.get(n.id)?.body) { toast('Add some content first'); return; }
    const body = n.drawMode ? drawBody : (_noteUnlocked.get(n.id)?.body || n.body || '');
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
    n.drawing = undefined;     // legacy plaintext PNG, if any
    n.drawStrokes = undefined; // plaintext strokes now live encrypted server-side
    n.drawPhoto = undefined; n.drawPhotoT = undefined; // photo too
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
  // Tick mode and draw mode are mutually exclusive. If we're drawing,
  // leave draw mode (artwork preserved) before turning ticks on.
  if (n.drawMode) {
    if (_noteDrawState && _noteDrawState._textEdit) _commitNoteTextEdit();
    n.drawMode = false;
    _noteDrawState = null;
    document.getElementById('note-btn-draw')?.classList.toggle('active', false);
    const drawWrap = document.getElementById('note-draw-wrap');
    if (drawWrap) drawWrap.style.display = 'none';
  }
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
  // Draw notes: copy the canvas image to the clipboard, not the sentinel.
  if (n.drawMode) {
    const canvas = document.getElementById('note-draw-canvas');
    if (canvas && navigator.clipboard?.write && window.ClipboardItem) {
      canvas.toBlob((blob) => {
        if (!blob) { toast('Nothing to copy'); return; }
        navigator.clipboard.write([new ClipboardItem({ 'image/png': blob })])
          .then(() => toast('Drawing copied ✓'))
          .catch(() => toast('Copy not supported here'));
      }, 'image/png');
    } else {
      toast('Copy not supported here');
    }
    return;
  }
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
// ═══════════════════════════════════════════════════════════════════
//  DRAWING / STYLUS MODE  (vector strokes)
// ═══════════════════════════════════════════════════════════════════
// Per-note hand-drawn mode. A note is EITHER text/ticks OR a drawing —
// toggleNoteDraw() switches between them (mutually exclusive, mirroring
// toggleNoteTicks).
//
// STORAGE — pure vector. The source of truth is a list of strokes, NOT a
// rasterised PNG. Each stroke records its tool, colour, width and a list
// of points in NORMALISED coordinates (0–1 against a unit space, scaled
// by n.drawAspect = width/height at draw time) so it redraws crisply at
// any canvas size and zoom.
//   n.drawStrokes : [ { t:'p', c:'#e8a838', w:5, pts:[[x,y,pr], …] }, … ]
//   n.drawAspect  : number (cssW / cssH) captured when first drawn
//   • unlocked notes → n.drawStrokes / n.drawAspect stored in plaintext
//   • locked notes   → JSON folded into the encrypted body via the
//     \u0001VDRAW\u0001 sentinel in _getCurrentEditorBody (rides /note/body/push)
//
// Benefits over the old PNG approach: per-stroke undo/redo with no bitmap
// snapshot stack, resolution-independent rendering (cards/sharing draw
// inline SVG straight from the strokes), recolouring, and far smaller
// encrypted payloads. The eraser DELETES whole strokes it touches.
//
// LEGACY: notes saved by the old PNG engine still carry n.drawing (a PNG
// data-URL) with no n.drawStrokes. Those still DISPLAY (card + viewer fall
// back to the PNG) but are not editable as vectors — opening one starts a
// fresh stroke list over the shown background is NOT done; instead we treat
// a legacy PNG note as read-only artwork until redrawn. See _noteHasVector.
//
// Input is via Pointer Events (stylus, touch, mouse) with pressure where
// the device reports it; coalesced events smooth high-frequency styli.

let _noteDrawState = null;  // { noteId, ctx, canvas, cssW, cssH, dpr,
                            //   tool, colour, size, drawing(bool),
                            //   strokes:[], history:[], redo:[], cur:null }
const _NOTE_DRAW_MAX_HISTORY = 100;  // undo/redo depth (stroke-list snapshots)

// True if the note has editable vector content — strokes or a photo
// backdrop (vs a legacy PNG or empty).
function _noteHasVector(n) {
  return !!(n && ((Array.isArray(n.drawStrokes) && n.drawStrokes.length) || n.drawPhoto));
}

// Build the \u0001VDRAW\u0001 body string from drawing parts. Single source of
// truth for the locked-note / sync payload so the two write sites can't
// drift. Omits empty fields to keep the payload tight.
function _serializeVDraw(strokes, aspect, photo, photoT) {
  // Clone elements so transient flags (e.g. a text element's _editing) never
  // get persisted into the saved/encrypted payload.
  const clean = (strokes || []).map(_cloneStroke);
  const obj = { s: clean, a: aspect || 1 };
  if (photo) { obj.p = photo; if (photoT) obj.pt = photoT; }
  return `\u0001VDRAW\u0001${JSON.stringify(obj)}`;
}

// Read a note's stroke list from wherever it lives (plaintext for unlocked
// notes, the \u0001VDRAW\u0001 sentinel body for unlocked-in-memory locked notes).
// Returns { strokes, aspect, photo, photoT } — photo is a compressed
// data-URL backdrop ('' if none), photoT its {x,y,scale} placement.
function _noteVectorData(n) {
  const unlocked = _noteUnlocked.get(n.id);
  const rawBody  = unlocked ? unlocked.body : null;
  if (rawBody && rawBody.startsWith('\u0001VDRAW\u0001')) {
    try {
      const obj = JSON.parse(rawBody.slice('\u0001VDRAW\u0001'.length));
      return { strokes: obj.s || [], aspect: obj.a || 1, photo: obj.p || '', photoT: obj.pt || null };
    } catch (_) { return { strokes: [], aspect: 1, photo: '', photoT: null }; }
  }
  return {
    strokes: Array.isArray(n.drawStrokes) ? n.drawStrokes : [],
    aspect: n.drawAspect || 1,
    photo: n.drawPhoto || '',
    photoT: n.drawPhotoT || null,
  };
}

// Legacy PNG fallback URL for display only (old engine notes). '' if none.
function _noteLegacyPngUrl(n) {
  const unlocked = _noteUnlocked.get(n.id);
  const rawBody  = unlocked ? unlocked.body : null;
  if (rawBody && rawBody.startsWith('\u0001DRAW\u0001')) {
    return rawBody.slice('\u0001DRAW\u0001'.length);
  }
  return n.drawing || '';
}

// Render a stroke list to an inline SVG string sized to fit a box, while
// preserving the drawing's original aspect ratio (aspect = cssW/cssH at
// draw time). Points are normalised per-axis; the "page" is the unit
// square [0,1]², but because zoom-out drawing can place strokes outside
// that range, we expand the rendered region to include all content so
// nothing is clipped in cards / the shared viewer / export.
function _strokesToSvg(strokes, aspect, boxW, boxH, bg, photo, photoT, photoNatAspect) {
  if ((!strokes || !strokes.length) && (!bg || bg === 'none') && !photo) return '';
  const a = aspect && aspect > 0 ? aspect : 1;  // width/height
  // Content bounds in normalised space, unioned with the unit page and the
  // photo extent so nothing is clipped.
  let minX = 0, minY = 0, maxX = 1, maxY = 1;
  for (const st of (strokes || [])) {
    if (st.t === 't') {
      const b = _textElemBounds(st);
      if (b) {
        if (b.x < minX) minX = b.x; if (b.x + b.w > maxX) maxX = b.x + b.w;
        if (b.y < minY) minY = b.y; if (b.y + b.h > maxY) maxY = b.y + b.h;
      }
      continue;
    }
    for (const p of (st.pts || [])) {
      if (p[0] < minX) minX = p[0]; if (p[0] > maxX) maxX = p[0];
      if (p[1] < minY) minY = p[1]; if (p[1] > maxY) maxY = p[1];
    }
  }
  const pna = photoNatAspect && photoNatAspect > 0 ? photoNatAspect : 1;
  if (photo && photoT) {
    const pw = photoT.scale;
    const ph = (photoT.scale / pna) * a; // height in normalised-x units * aspect → normalised-y
    minX = Math.min(minX, photoT.x); minY = Math.min(minY, photoT.y);
    maxX = Math.max(maxX, photoT.x + pw); maxY = Math.max(maxY, photoT.y + ph);
  }
  const spanX = maxX - minX, spanY = maxY - minY;
  // Fit the content span (width:height = spanX*a : spanY) into the box.
  const contentAspect = (spanX * a) / spanY;
  let w = boxW, h = boxW / contentAspect;
  if (h > boxH) { h = boxH; w = boxH * contentAspect; }
  // Photo first (behind), then guides, then strokes.
  let photoFrag = '';
  if (photo && photoT) {
    const pxw = (photoT.scale / spanX) * w;
    const pxh = (((photoT.scale / pna) * a) / spanY) * h;
    const pxx = ((photoT.x - minX) / spanX) * w;
    const pxy = ((photoT.y - minY) / spanY) * h;
    photoFrag = `<image href="${photo}" x="${pxx.toFixed(1)}" y="${pxy.toFixed(1)}" width="${pxw.toFixed(1)}" height="${pxh.toFixed(1)}" preserveAspectRatio="none"/>`;
  }
  const bgFrag = _noteDrawBgSvg(bg, w, h, Math.max(10, w / 14));
  let paths = '';
  for (const st of (strokes || [])) {
    if (st.t === 't') {
      if (!st.str) continue;
      const fpx = (st.fs || 0.04) * h * (1 / spanY);  // font px in the scaled box
      const tx = ((st.x - minX) / spanX) * w;
      let ty = ((st.y - minY) / spanY) * h;
      const lines = String(st.str).split('\n');
      const rotAttr = st.rot ? ` transform="rotate(${(st.rot * 180 / Math.PI).toFixed(2)} ${tx.toFixed(1)} ${ty.toFixed(1)})"` : '';
      let inner = '';
      for (const ln of lines) {
        inner += `<text x="${tx.toFixed(1)}" y="${(ty + fpx).toFixed(1)}" font-family="system-ui, sans-serif" font-weight="600" font-size="${fpx.toFixed(1)}" fill="${esc(st.c || '#e8a838')}">${esc(ln)}</text>`;
        ty += fpx * 1.25;
      }
      paths += rotAttr ? `<g${rotAttr}>${inner}</g>` : inner;
      continue;
    }
    if (!st.pts || !st.pts.length) continue;
    const sw = Math.max(0.6, (st.w || 5) * (w / 320) * 1.0);
    let d = '';
    for (let i = 0; i < st.pts.length; i++) {
      const px = ((st.pts[i][0] - minX) / spanX) * w;
      const py = ((st.pts[i][1] - minY) / spanY) * h;
      d += (i === 0 ? 'M' : 'L') + px.toFixed(1) + ',' + py.toFixed(1) + ' ';
    }
    paths += `<path d="${d.trim()}" fill="none" stroke="${esc(st.c || '#e8a838')}" stroke-width="${sw.toFixed(1)}" stroke-linecap="round" stroke-linejoin="round"/>`;
  }
  return `<svg width="${w.toFixed(0)}" height="${h.toFixed(0)}" viewBox="0 0 ${w.toFixed(1)} ${h.toFixed(1)}" xmlns="http://www.w3.org/2000/svg" style="max-width:100%;height:auto">${photoFrag}${bgFrag}${paths}</svg>`;
}

// ── Background guides (ruled / grid / dots) ──────────────────────────
// Stored as n.drawBg (cosmetic enum, not part of the stroke data, so it
// costs no storage and is plaintext even on locked notes). Rendered three
// ways: as a CSS background on the canvas host (live editor), and as SVG
// rects/lines/dots in _strokesToSvg + the export canvas, so cards, the
// shared viewer and exported PNGs all match what the user drew on.
const _NOTE_DRAW_BG_ORDER = ['none', 'ruled', 'grid', 'dots'];
const _NOTE_DRAW_BG_LABEL = { none: 'None', ruled: 'Ruled', grid: 'Grid', dots: 'Dots' };

// CSS background-image value for a given style. Uses a muted line colour so
// guides sit behind the art in any theme. `step` is the grid spacing in px.
function _noteDrawBgCss(style, step) {
  const s = step || 26;
  const line = 'var(--border)';
  if (style === 'ruled') {
    return { backgroundImage: `repeating-linear-gradient(to bottom, transparent 0, transparent ${s-1}px, ${line} ${s-1}px, ${line} ${s}px)`, backgroundSize: 'auto' };
  }
  if (style === 'grid') {
    return { backgroundImage: `repeating-linear-gradient(to bottom, transparent 0, transparent ${s-1}px, ${line} ${s-1}px, ${line} ${s}px), repeating-linear-gradient(to right, transparent 0, transparent ${s-1}px, ${line} ${s-1}px, ${line} ${s}px)`, backgroundSize: 'auto' };
  }
  if (style === 'dots') {
    return { backgroundImage: `radial-gradient(${line} 1.2px, transparent 1.3px)`, backgroundSize: `${s}px ${s}px` };
  }
  return { backgroundImage: 'none', backgroundSize: 'auto' };
}

function _applyNoteDrawBg(style, view, cssW, cssH) {
  const host = document.getElementById('note-draw-canvas-host');
  if (!host) return;
  // When a photo backdrop is present, guides are painted on-canvas (over the
  // photo) by _redrawNoteStrokes, so clear the CSS host guides to avoid
  // double-drawing them behind the photo.
  if (_noteDrawState && _noteDrawState.photoData) {
    host.style.backgroundImage = 'none';
    return;
  }
  const v = view || (_noteDrawState && _noteDrawState.view) || { zoom: 1, panX: 0, panY: 0 };
  const baseStep = 26;
  const step = baseStep * v.zoom;  // guides scale with zoom to track strokes
  const css = _noteDrawBgCss(style, step);
  host.style.backgroundImage = css.backgroundImage;
  // For ruled/grid the size is the step; for dots it's set in _noteDrawBgCss.
  if (style === 'dots') {
    host.style.backgroundSize = `${step}px ${step}px`;
  } else if (style === 'ruled' || style === 'grid') {
    host.style.backgroundSize = 'auto';  // repeating-gradient uses px in image
  } else {
    host.style.backgroundSize = 'auto';
  }
  // Offset the pattern to follow panning. The grid origin in stored space is
  // 0; on screen that's at px = -panX*zoom*cssW (and same for Y).
  const w = cssW || (_noteDrawState && _noteDrawState.cssW) || 1;
  const h = cssH || (_noteDrawState && _noteDrawState.cssH) || 1;
  const ox = -(v.panX * v.zoom * w);
  const oy = -(v.panY * v.zoom * h);
  host.style.backgroundPosition = `${ox.toFixed(1)}px ${oy.toFixed(1)}px`;
}

// ── Pan & zoom controls ──────────────────────────────────────────────
// Toggle the Hand tool: when on, a one-finger/mouse drag pans instead of
// drawing. Two-finger pinch/pan and wheel-zoom work regardless of this.
function toggleNoteDrawHand() {
  const s = _noteDrawState; if (!s) return;
  s.handMode = !s.handMode;
  const btn = document.getElementById('note-draw-hand');
  if (btn) btn.classList.toggle('active', s.handMode);
  const cv = document.getElementById('note-draw-canvas');
  if (cv) cv.classList.toggle('hand', s.handMode);
}

// Straightedge: while on, every stroke is forced to a straight line.
function toggleNoteDrawRuler() {
  const s = _noteDrawState; if (!s) return;
  s.rulerMode = !s.rulerMode;
  const btn = document.getElementById('note-draw-ruler');
  if (btn) btn.classList.toggle('active', s.rulerMode);
  toast(s.rulerMode ? 'Ruler on — strokes snap to straight lines' : 'Ruler off');
}

// Reset pan & zoom to the default 1:1 view.
function resetNoteDrawView() {
  const s = _noteDrawState; if (!s) return;
  s.view = { zoom: 1, panX: 0, panY: 0 };
  _applyNoteDrawBg((notes.find(x => x.id === _editingNoteId) || {}).drawBg || 'none', s.view, s.cssW, s.cssH);
  _redrawNoteStrokes();
}

// Update the on-screen zoom % readout, and show/hide the reset button.
function _updateNoteDrawZoomReadout() {
  const s = _noteDrawState; if (!s) return;
  const el = document.getElementById('note-draw-zoom');
  if (el) el.textContent = Math.round(s.view.zoom * 100) + '%';
  const reset = document.getElementById('note-draw-reset-view');
  if (reset) {
    const atDefault = Math.abs(s.view.zoom - 1) < 0.001 && Math.abs(s.view.panX) < 0.001 && Math.abs(s.view.panY) < 0.001;
    reset.style.display = atDefault ? 'none' : '';
  }
}

// ── Photo backdrop (insert / compress / place) ───────────────────────
const _NOTE_PHOTO_MAX_EDGE = 1600;   // downscale long edge to this
const _NOTE_PHOTO_QUALITY  = 0.7;    // WebP/JPEG quality
const _NOTE_PHOTO_WARN     = 800 * 1024;   // warn above ~800 KB
const _NOTE_PHOTO_REFUSE   = 3 * 1024 * 1024; // hard cap ~3 MB

// Open the file picker to insert a photo backdrop.
function insertNoteDrawPhoto() {
  const s = _noteDrawState; if (!s) return;
  let input = document.getElementById('note-draw-photo-input');
  if (!input) {
    input = document.createElement('input');
    input.type = 'file';
    input.accept = 'image/*';
    input.id = 'note-draw-photo-input';
    input.style.display = 'none';
    input.addEventListener('change', _onNoteDrawPhotoChosen);
    document.body.appendChild(input);
  }
  input.value = '';
  input.click();
}

async function _onNoteDrawPhotoChosen(e) {
  const file = e.target.files && e.target.files[0];
  if (!file) return;
  if (!/^image\//.test(file.type)) { toast('Please choose an image'); return; }
  toast('Processing photo…');
  try {
    const { dataUrl, bytes, natAspect } = await _compressNotePhoto(file);
    if (bytes > _NOTE_PHOTO_REFUSE) {
      toast('Photo is too large even after compression — try a smaller image');
      return;
    }
    if (bytes > _NOTE_PHOTO_WARN) {
      const kb = Math.round(bytes / 1024);
      if (!confirm(`This photo is ${kb} KB after compression and will make the note heavier to sync. Add it anyway?`)) return;
    }
    const s = _noteDrawState; if (!s) return;
    s.photoData = dataUrl;
    s.photoNatAspect = natAspect;
    // Default placement: fit the photo to the canvas width, centred vertically.
    const fitH = (1 / natAspect);  // height in normalised-x units when width = 1
    s.photoT = { x: 0, y: Math.max(0, (1 - fitH * (s.cssW / s.cssH)) / 2), scale: 1, na: natAspect };
    const img = new Image();
    img.onload = () => {
      if (_noteDrawState === s) {
        s.photoImg = img;
        s.placingPhoto = true;
        _applyNoteDrawBg((notes.find(x => x.id === _editingNoteId) || {}).drawBg || 'none', s.view, s.cssW, s.cssH);
        _updateNoteDrawPhotoUI();
        _redrawNoteStrokes();
        toast('Drag to move, pinch the photo to resize, then tap ✓');
      }
    };
    img.src = dataUrl;
  } catch (err) {
    toast('Could not process photo');
    console.error('[note photo] compress error:', err);
  }
}

// Downscale + re-encode an image File to a compact data-URL. Prefers WebP
// (much smaller for photos) with a JPEG fallback. Returns the data-URL, its
// byte size, and the natural aspect ratio.
// NOTE: the source image is loaded via a FileReader `data:` URL rather than
// URL.createObjectURL(). The app's Content-Security-Policy allows
// `img-src 'self' data: https:` but NOT `blob:`, so an object URL trips a
// CSP violation. A data: URL is permitted and needs no cleanup.
function _compressNotePhoto(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onerror = () => reject(new Error('read failed'));
    reader.onload = () => {
      const img = new Image();
      img.onload = () => {
        try {
          let w = img.naturalWidth, h = img.naturalHeight;
          const long = Math.max(w, h);
          if (long > _NOTE_PHOTO_MAX_EDGE) {
            const k = _NOTE_PHOTO_MAX_EDGE / long;
            w = Math.round(w * k); h = Math.round(h * k);
          }
          const cv = document.createElement('canvas');
          cv.width = w; cv.height = h;
          const cx = cv.getContext('2d');
          cx.drawImage(img, 0, 0, w, h);
          // Try WebP; if unsupported the result won't start with data:image/webp.
          let dataUrl = cv.toDataURL('image/webp', _NOTE_PHOTO_QUALITY);
          if (!/^data:image\/webp/.test(dataUrl)) {
            dataUrl = cv.toDataURL('image/jpeg', _NOTE_PHOTO_QUALITY);
          }
          // Approx byte size of the base64 payload.
          const b64 = dataUrl.split(',')[1] || '';
          const bytes = Math.floor(b64.length * 3 / 4);
          resolve({ dataUrl, bytes, natAspect: w / Math.max(1, h) });
        } catch (e) { reject(e); }
      };
      img.onerror = () => reject(new Error('decode failed'));
      img.src = reader.result;  // data: URL — CSP-safe
    };
    reader.readAsDataURL(file);
  });
}

// Finish placing the photo: lock it in as the backdrop and persist.
function finishNoteDrawPhotoPlacement() {
  const s = _noteDrawState; if (!s) return;
  s.placingPhoto = false;
  _updateNoteDrawPhotoUI();
  _redrawNoteStrokes();
  const n = notes.find(x => x.id === _editingNoteId);
  if (n && !n.locked) { n.drawPhoto = s.photoData || null; n.drawPhotoT = s.photoT || null; }
  _noteBodyDirty = true;
  clearTimeout(_noteAutoSaveTimer);
  _noteAutoSaveTimer = setTimeout(_autoSaveNote, 400);
}

function removeNoteDrawPhoto() {
  const s = _noteDrawState; if (!s) return;
  if (!s.photoData) return;
  if (!confirm('Remove the photo backdrop? Your drawing stays.')) return;
  s.photoData = null; s.photoImg = null; s.photoT = null; s.placingPhoto = false;
  const n = notes.find(x => x.id === _editingNoteId);
  if (n && !n.locked) { n.drawPhoto = null; n.drawPhotoT = null; }
  _applyNoteDrawBg((n || {}).drawBg || 'none', s.view, s.cssW, s.cssH);
  _updateNoteDrawPhotoUI();
  _redrawNoteStrokes();
  _noteBodyDirty = true;
  clearTimeout(_noteAutoSaveTimer);
  _noteAutoSaveTimer = setTimeout(_autoSaveNote, 400);
}

// Show/hide the photo-related toolbar buttons based on current state.
function _updateNoteDrawPhotoUI() {
  const s = _noteDrawState;
  const removeBtn = document.getElementById('note-draw-photo-remove');
  const doneBtn   = document.getElementById('note-draw-photo-done');
  if (removeBtn) removeBtn.style.display = (s && s.photoData) ? '' : 'none';
  if (doneBtn)   doneBtn.style.display   = (s && s.placingPhoto) ? '' : 'none';
}

async function cycleNoteDrawBg() {
  const n = notes.find(x => x.id === _editingNoteId); if (!n) return;
  const cur = n.drawBg || 'none';
  const next = _NOTE_DRAW_BG_ORDER[(_NOTE_DRAW_BG_ORDER.indexOf(cur) + 1) % _NOTE_DRAW_BG_ORDER.length];
  n.drawBg = next;
  _applyNoteDrawBg(next);
  const btn = document.getElementById('note-draw-bg');
  if (btn) btn.title = `Background: ${_NOTE_DRAW_BG_LABEL[next]} (tap to change)`;
  toast(`Background: ${_NOTE_DRAW_BG_LABEL[next]}`);
  n.updatedAt = new Date().toISOString();
  _noteBodyDirty = true;
  clearTimeout(_noteAutoSaveTimer);
  _noteAutoSaveTimer = setTimeout(_autoSaveNote, 600);
}

// SVG fragment for the background guides, sized to a w×h box. Returned as a
// <defs>+<rect> pair to prepend inside an existing SVG. Empty for 'none'.
function _noteDrawBgSvg(style, w, h, step) {
  if (!style || style === 'none') return '';
  const s = step || 26;
  const line = '#888';
  const op = 0.28;
  if (style === 'dots') {
    return `<defs><pattern id="ndbg" width="${s}" height="${s}" patternUnits="userSpaceOnUse"><circle cx="1.3" cy="1.3" r="1.1" fill="${line}" opacity="${op}"/></pattern></defs><rect width="${w.toFixed(1)}" height="${h.toFixed(1)}" fill="url(#ndbg)"/>`;
  }
  let lines = '';
  for (let y = s; y < h; y += s) lines += `<line x1="0" y1="${y}" x2="${w.toFixed(1)}" y2="${y}" stroke="${line}" stroke-width="0.6" opacity="${op}"/>`;
  if (style === 'grid') {
    for (let x = s; x < w; x += s) lines += `<line x1="${x}" y1="0" x2="${x}" y2="${h.toFixed(1)}" stroke="${line}" stroke-width="0.6" opacity="${op}"/>`;
  }
  return lines;
}

// Paint background guides directly onto the (already view-transformed) live
// canvas, covering a generous normalised range so they fill the viewport at
// any pan/zoom. Used only when a photo backdrop is present, so guides sit
// above the photo; otherwise the cheaper CSS host background is used.
function _paintGuidesOnCanvas(ctx, style, cssW, cssH, view) {
  if (!style || style === 'none') return;
  const baseStep = 26; // css px at zoom 1, matching the CSS guides
  const stepX = baseStep / cssW;  // normalised
  const stepY = baseStep / cssH;
  ctx.save();
  ctx.strokeStyle = 'rgba(136,136,136,0.30)';
  ctx.fillStyle   = 'rgba(136,136,136,0.30)';
  ctx.lineWidth = 1 / (view ? view.zoom : 1);
  ctx.setLineDash([]);
  const from = -1, to = 2;  // cover well beyond the unit page
  if (style === 'dots') {
    for (let gy = 0; gy <= to; gy += stepY) { for (let gx = 0; gx <= to; gx += stepX) {
      ctx.beginPath(); ctx.arc(gx * cssW, gy * cssH, Math.max(1, 1.2 / (view ? view.zoom : 1)), 0, Math.PI*2); ctx.fill();
    } }
  } else {
    for (let gy = 0; gy <= to; gy += stepY) {
      ctx.beginPath(); ctx.moveTo(from * cssW, gy * cssH); ctx.lineTo(to * cssW, gy * cssH); ctx.stroke();
    }
    if (style === 'grid') for (let gx = 0; gx <= to; gx += stepX) {
      ctx.beginPath(); ctx.moveTo(gx * cssW, from * cssH); ctx.lineTo(gx * cssW, to * cssH); ctx.stroke();
    }
  }
  ctx.restore();
}

// Export the current drawing as a PNG file. Rasterises strokes + the
// background guide onto an offscreen canvas at a fixed high resolution
// (independent of the on-screen canvas size, since strokes are vector),
// then triggers a download. Works for locked notes too — it reads the
// live in-memory strokes from _noteDrawState.
function exportNoteDrawingPng() {
  const n = notes.find(x => x.id === _editingNoteId); if (!n) return;
  const s = (_noteDrawState && _noteDrawState.noteId === n.id) ? _noteDrawState : null;
  const strokes = s ? s.strokes : _noteVectorData(n).strokes;
  const aspect  = s ? s.aspect  : (_noteVectorData(n).aspect || 1);
  const photoImg = s ? s.photoImg : null;
  const photoT   = s ? s.photoT   : (_noteVectorData(n).photoT || null);
  if ((!strokes || !strokes.length) && !photoImg) { toast('Nothing to export'); return; }

  // Content bounds (union with the unit page, photo extent, and any text).
  let minX = 0, minY = 0, maxX = 1, maxY = 1;
  for (const st of strokes) {
    if (st.t === 't') {
      const b = _textElemBounds(st);
      if (b) {
        if (b.x < minX) minX = b.x; if (b.x + b.w > maxX) maxX = b.x + b.w;
        if (b.y < minY) minY = b.y; if (b.y + b.h > maxY) maxY = b.y + b.h;
      }
      continue;
    }
    for (const p of (st.pts || [])) {
      if (p[0] < minX) minX = p[0]; if (p[0] > maxX) maxX = p[0];
      if (p[1] < minY) minY = p[1]; if (p[1] > maxY) maxY = p[1];
    }
  }
  if (photoImg && photoT) {
    const pw = photoT.scale;
    const ph = (photoT.scale / (s.photoNatAspect || 1)) * (s.cssW / s.cssH);
    minX = Math.min(minX, photoT.x); minY = Math.min(minY, photoT.y);
    maxX = Math.max(maxX, photoT.x + pw); maxY = Math.max(maxY, photoT.y + ph);
  }
  const spanX = maxX - minX, spanY = maxY - minY;
  const contentAspect = (spanX * aspect) / spanY;

  // Target ~1600px on the long edge for a crisp export.
  const long = 1600;
  let w, h;
  if (contentAspect >= 1) { w = long; h = Math.round(long / contentAspect); }
  else { h = long; w = Math.round(long * contentAspect); }

  const cv = document.createElement('canvas');
  cv.width = w; cv.height = h;
  const ctx = cv.getContext('2d');
  // Opaque background so the PNG isn't transparent. Use the app surface
  // colour resolved from CSS so it matches the editor.
  const surface = getComputedStyle(document.body).getPropertyValue('--bg').trim() || '#0d0d0d';
  ctx.fillStyle = surface || '#0d0d0d';
  ctx.fillRect(0, 0, w, h);

  const mapX = (nx) => ((nx - minX) / spanX) * w;
  const mapY = (ny) => ((ny - minY) / spanY) * h;

  // Photo backdrop (behind guides and strokes).
  if (photoImg && photoT) {
    const pw = photoT.scale, ph = (photoT.scale / (s.photoNatAspect || 1)) * (s.cssW / s.cssH);
    try { ctx.drawImage(photoImg, mapX(photoT.x), mapY(photoT.y), (pw / spanX) * w, (ph / spanY) * h); } catch (_) {}
  }

  // Background guides.
  _paintBgOnCanvas(ctx, n.drawBg, w, h);

  // Strokes + text (map content bounds → export size).
  ctx.lineCap = 'round'; ctx.lineJoin = 'round';
  for (const st of strokes) {
    if (st.t === 't') {
      if (!st.str) continue;
      const fpx = (st.fs || 0.04) * (h / spanY);
      ctx.save();
      ctx.fillStyle = st.c || '#e8a838';
      ctx.font = `600 ${fpx}px system-ui, sans-serif`;
      ctx.textBaseline = 'top';
      const ax = mapX(st.x), ay0 = mapY(st.y);
      if (st.rot) { ctx.translate(ax, ay0); ctx.rotate(st.rot); ctx.translate(-ax, -ay0); }
      let ty = ay0;
      for (const ln of String(st.str).split('\n')) { ctx.fillText(ln, ax, ty); ty += fpx * 1.25; }
      ctx.restore();
      continue;
    }
    ctx.strokeStyle = st.c || '#e8a838';
    ctx.fillStyle   = st.c || '#e8a838';
    const scale = w / 320;  // match the on-screen stroke scaling feel
    for (let i = 1; i < st.pts.length; i++) {
      const x0 = mapX(st.pts[i-1][0]), y0 = mapY(st.pts[i-1][1]);
      const x1 = mapX(st.pts[i][0]),   y1 = mapY(st.pts[i][1]);
      const pr = st.pts[i][2] != null ? st.pts[i][2] : 0.5;
      ctx.lineWidth = (st.w || 5) * (0.5 + pr) * scale * 0.5;
      ctx.beginPath(); ctx.moveTo(x0, y0); ctx.lineTo(x1, y1); ctx.stroke();
    }
    if (st.pts.length === 1) {
      const x = mapX(st.pts[0][0]), y = mapY(st.pts[0][1]);
      const pr = st.pts[0][2] != null ? st.pts[0][2] : 0.5;
      ctx.beginPath();
      ctx.arc(x, y, ((st.w || 5) * (0.5 + pr) * (w/320) * 0.5) / 2, 0, Math.PI*2);
      ctx.fill();
    }
  }

  const safeTitle = (n.title || 'drawing').replace(/[^a-z0-9_-]+/gi, '_').slice(0, 40) || 'drawing';
  cv.toBlob((blob) => {
    if (!blob) { toast('Export failed'); return; }
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = `${safeTitle}.png`;
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 4000);
    toast('Drawing exported ✓');
  }, 'image/png');
}

// Paint background guides onto an arbitrary 2D context (used by export).
function _paintBgOnCanvas(ctx, style, w, h) {
  if (!style || style === 'none') return;
  const step = Math.max(14, Math.round(w / 26));
  ctx.save();
  ctx.strokeStyle = 'rgba(136,136,136,0.28)';
  ctx.fillStyle   = 'rgba(136,136,136,0.28)';
  ctx.lineWidth = Math.max(1, w / 1400);
  if (style === 'dots') {
    for (let y = step; y < h; y += step)
      for (let x = step; x < w; x += step) { ctx.beginPath(); ctx.arc(x, y, Math.max(1.2, w/900), 0, Math.PI*2); ctx.fill(); }
  } else {
    for (let y = step; y < h; y += step) { ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(w, y); ctx.stroke(); }
    if (style === 'grid')
      for (let x = step; x < w; x += step) { ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, h); ctx.stroke(); }
  }
  ctx.restore();
}

function _initNoteDrawCanvas(n) {
  const canvas = document.getElementById('note-draw-canvas');
  const host   = document.getElementById('note-draw-canvas-host');
  if (!canvas || !host) return;

  const dpr = window.devicePixelRatio || 1;
  const rect = host.getBoundingClientRect();
  const w = Math.max(1, Math.round(rect.width));
  const h = Math.max(1, Math.round(rect.height));

  canvas.width  = w * dpr;
  canvas.height = h * dpr;
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.lineCap = 'round';
  ctx.lineJoin = 'round';

  // Load existing strokes (preserve in-flight edits if re-initing same note).
  const keep = (_noteDrawState && _noteDrawState.noteId === n.id) ? _noteDrawState : null;
  let strokes, redo, history;
  if (keep) {
    strokes = keep.strokes; redo = keep.redo; history = keep.history;
  } else {
    const vd = _noteVectorData(n);
    strokes = vd.strokes.map(_cloneStroke);  // own copy so undo is isolated
    redo = [];
    history = [];
  }

  _noteDrawState = {
    noteId: n.id,
    ctx, canvas,
    cssW: w, cssH: h, dpr,
    aspect: w / Math.max(1, h),
    tool:   keep ? keep.tool   : 'pen',
    colour: keep ? keep.colour : '#e8a838',
    size:   keep ? keep.size   : 5,
    drawing: false,
    strokes,
    history,             // snapshots of the stroke list for undo
    redo,                // snapshots for redo
    cur: null,           // stroke currently being drawn
    _preGesture: null,   // pre-erase-gesture snapshot
    erasedSomething: false,
    // View transform (session-only, never persisted). A stored normalised
    // point (nx,ny) maps to canvas px: ((n - pan) * zoom) * cssSize.
    // Strokes are always stored untransformed; this only affects what's
    // shown and where input lands while editing.
    view: keep ? keep.view : { zoom: 1, panX: 0, panY: 0 },
    handMode: keep ? keep.handMode : false,  // Hand-tool toggle (one-finger pan)
    rulerMode: keep ? keep.rulerMode : false, // Straightedge: force strokes to straight lines
    _gesture: null,      // active pinch/two-finger gesture state
    // Palm rejection (Auto): once a stylus (pen pointer) is used, touch
    // input stops drawing and only pans. Reverts to finger-draw a few
    // seconds after the pen is set down. lastPenAt = ms of last pen activity.
    penDown: 0,          // count of active pen pointers
    lastPenAt: 0,        // performance.now() of last pen down/move
    _activePointerId: null,  // pointer that owns the in-progress stroke
    // Photo backdrop: photoData is the compressed data-URL, photoImg the
    // decoded <img> for painting, photoT the placement {x,y,scale} in
    // normalised space (x,y = top-left in 0–1 units, scale relative to a
    // width-fit). placingPhoto = true while the user is moving/scaling it.
    photoData: null,
    photoImg: null,
    photoT: null,
    photoNatAspect: 1,   // natural width/height of the photo
    placingPhoto: false,
    // Text tool: its own colour + size (normalised height fraction). While a
    // text element is being typed, _textEdit holds {idx, x, y} of the element
    // being created/edited so commit/cancel know what to write back.
    textColour: keep ? keep.textColour : '#ffffff',
    textSize:   keep ? keep.textSize   : 0.045,  // normalised-height units
    _textEdit:  null,
    _textDrag:  null,    // dragging an existing text element
    // Select + transform tool: _sel is an array of selected element indices;
    // _selXf is the active transform gesture (move/scale/rotate); _marquee is
    // an in-progress selection box {x0,y0,x1,y1} in normalised space.
    _sel: [],
    _selXf: null,
    _marquee: null,
  };

  // Load any existing photo backdrop for this note.
  {
    const vd = _noteVectorData(n);
    if (vd && vd.photo) {
      _noteDrawState.photoData = vd.photo;
      _noteDrawState.photoT = vd.photoT || { x: 0, y: 0, scale: 1 };
      // Seed natural aspect from the stored placement so SVG/sizing is right
      // even before the image finishes decoding.
      if (vd.photoT && vd.photoT.na) _noteDrawState.photoNatAspect = vd.photoT.na;
      const img = new Image();
      img.onload = () => {
        if (_noteDrawState && _noteDrawState.noteId === n.id) {
          _noteDrawState.photoImg = img;
          _noteDrawState.photoNatAspect = img.naturalWidth / Math.max(1, img.naturalHeight);
          _redrawNoteStrokes();
        }
      };
      img.src = vd.photo;
    }
  }

  // Legacy PNG notes (old engine, no vector strokes) are not editable as
  // vectors — opening one starts an empty stroke list. The old PNG still
  // shows on the card and shared viewer via the fallback path, but we don't
  // paint it into the editor canvas because the first new stroke's redraw
  // would clear it, which looks like a glitch. Drawing here creates fresh
  // vector art that becomes the source of truth on save.

  _redrawNoteStrokes();
  _applyNoteDrawBg(n.drawBg || 'none', _noteDrawState.view, w, h);
  const bgBtn = document.getElementById('note-draw-bg');
  if (bgBtn) bgBtn.title = `Background: ${_NOTE_DRAW_BG_LABEL[n.drawBg || 'none']} (tap to change)`;
  const handBtn = document.getElementById('note-draw-hand');
  if (handBtn) handBtn.classList.toggle('active', !!_noteDrawState.handMode);
  if (canvas) canvas.classList.toggle('hand', !!_noteDrawState.handMode);
  const rulerBtn = document.getElementById('note-draw-ruler');
  if (rulerBtn) rulerBtn.classList.toggle('active', !!_noteDrawState.rulerMode);
  _bindNoteDrawPointer(canvas);
  _updateNoteDrawToolUI();
  _updateNoteDrawUndoRedoBtns();
  _updateNoteDrawZoomReadout();
  _updateNoteDrawPhotoUI();
  _updateNoteDrawTextControls();
}

function _cloneStroke(st) {
  // Text element: { t:'t', x, y, str, fs, c, rot? }. Strokes: { t:'p', c, w, pts }.
  if (st.t === 't') {
    const o = { t: 't', x: st.x, y: st.y, str: st.str, fs: st.fs, c: st.c };
    if (st.rot) o.rot = st.rot;
    return o;
  }
  return { t: st.t || 'p', c: st.c, w: st.w, pts: st.pts.map(p => p.slice()) };
}

// ── View transform (pan & zoom) ──────────────────────────────────────
// Convert a screen pixel (relative to the canvas) to a stored normalised
// coordinate, accounting for the current pan/zoom. Inverse of the paint
// transform applied in _redrawNoteStrokes.
function _screenToNorm(s, px, py) {
  const v = s.view;
  return [
    (px / s.cssW) / v.zoom + v.panX,
    (py / s.cssH) / v.zoom + v.panY,
  ];
}

// Clamp zoom to a sane range and keep panX/panY from drifting absurdly far.
function _clampView(v) {
  v.zoom = Math.max(0.25, Math.min(8, v.zoom));
  // Allow panning into margin (for zoom-out drawing surface) but not to
  // infinity — keep the unit square at least partly reachable.
  const lim = 3;
  v.panX = Math.max(-lim, Math.min(lim, v.panX));
  v.panY = Math.max(-lim, Math.min(lim, v.panY));
  return v;
}

// Palm rejection (Auto). Returns true when stylus input is currently the
// active drawing modality — i.e. a pen is down now, or one was down within
// the recent window. While true, touch input is demoted to pan-only so a
// resting palm or stray finger can't ink. Reverts automatically once the
// pen has been idle past the window, restoring finger-draw.
const _NOTE_DRAW_PEN_WINDOW_MS = 2500;
function _noteDrawStylusActive(s) {
  if (!s) return false;
  if (s.penDown > 0) return true;
  return (performance.now() - (s.lastPenAt || 0)) < _NOTE_DRAW_PEN_WINDOW_MS;
}

// Repaint the whole canvas from the stroke list. Cheap for typical notes
// (a few hundred segments); called on every commit, undo/redo, erase, and
// on pan/zoom. The view transform is applied here so _paintStroke stays in
// simple normalised×cssSize space.
function _redrawNoteStrokes() {
  const s = _noteDrawState; if (!s) return;
  const ctx = s.ctx;
  const v = s.view;
  // Reset to device-pixel transform, clear, then layer the view transform.
  ctx.setTransform(s.dpr, 0, 0, s.dpr, 0, 0);
  ctx.clearRect(0, 0, s.cssW, s.cssH);
  ctx.globalCompositeOperation = 'source-over';
  // Apply pan & zoom: translate by -pan*zoom*cssSize then scale by zoom.
  // Combined with dpr so lines stay crisp.
  ctx.setTransform(
    s.dpr * v.zoom, 0,
    0, s.dpr * v.zoom,
    -v.panX * v.zoom * s.cssW * s.dpr,
    -v.panY * v.zoom * s.cssH * s.dpr,
  );
  // Photo backdrop (behind strokes). Placed via photoT {x,y,scale} in
  // normalised units: width = scale (1 = canvas width), height keeps the
  // photo's natural aspect. Painted under the same view transform so it
  // pans/zooms with the drawing.
  if (s.photoImg && s.photoT) {
    const pw = s.photoT.scale * s.cssW;
    const ph = (s.photoT.scale / (s.photoNatAspect || 1)) * s.cssW;
    const px = s.photoT.x * s.cssW;
    const py = s.photoT.y * s.cssH;
    ctx.save();
    if (s.placingPhoto) ctx.globalAlpha = 0.92;
    try { ctx.drawImage(s.photoImg, px, py, pw, ph); } catch (_) {}
    ctx.restore();
    // While placing, outline the photo bounds so the user sees its extent.
    if (s.placingPhoto) {
      ctx.save();
      ctx.strokeStyle = 'var(--accent)';
      ctx.strokeStyle = getComputedStyle(document.body).getPropertyValue('--accent').trim() || '#e8a838';
      ctx.lineWidth = 2 / v.zoom;
      ctx.setLineDash([6 / v.zoom, 4 / v.zoom]);
      ctx.strokeRect(px, py, pw, ph);
      ctx.restore();
    }
  }
  // Background guides over the photo (when a photo is present we draw guides
  // on-canvas so they sit above the image; without a photo the cheaper CSS
  // host background is used instead, see _applyNoteDrawBg).
  if (s.photoImg) {
    const n2 = notes.find(x => x.id === s.noteId);
    _paintGuidesOnCanvas(ctx, (n2 && n2.drawBg) || 'none', s.cssW, s.cssH, v);
  }
  for (const st of s.strokes) _paintStroke(ctx, st, s.cssW, s.cssH, s.aspect);
  // Paint the in-progress stroke last so it appears on top while drawing.
  // In ruler mode, preview it as a straight line (matching what commit does).
  if (s.cur) {
    const preview = (s.rulerMode && s.cur.pts && s.cur.pts.length >= 2)
      ? { t: 'p', c: s.cur.c, w: s.cur.w, pts: _forceStraightLine(s.cur.pts) }
      : s.cur;
    _paintStroke(ctx, preview, s.cssW, s.cssH, s.aspect);
  }
  _updateNoteDrawZoomReadout();
  _paintSelectionOverlay(ctx, s, v);
}

// Hit-test the selection handles at a screen-space point (sx,sy in css px,
// already adjusted for the canvas origin). Returns 'nw'|'ne'|'se'|'sw'
// (scale corners), 'rotate', 'move' (inside box), or null. Uses the current
// view to map the normalised selection bounds to screen px.
function _selHandleAt(s, v, sx, sy) {
  if (!s._sel || !s._sel.length) return null;
  const b = _selectionBounds(s._sel); if (!b) return null;
  const X = (nx) => (nx - v.panX) * v.zoom * s.cssW;
  const Y = (ny) => (ny - v.panY) * v.zoom * s.cssH;
  const x = X(b.x), y = Y(b.y), w = b.w * v.zoom * s.cssW, h = b.h * v.zoom * s.cssH;
  const tol = 12;
  const near = (px, py) => Math.abs(sx - px) <= tol && Math.abs(sy - py) <= tol;
  if (near(x, y)) return 'nw';
  if (near(x + w, y)) return 'ne';
  if (near(x + w, y + h)) return 'se';
  if (near(x, y + h)) return 'sw';
  if (near(x + w / 2, y - 24)) return 'rotate';
  if (sx >= x - tol && sx <= x + w + tol && sy >= y - tol && sy <= y + h + tol) return 'move';
  return null;
}

// Draw the selection bounding box with scale handles (corners) and a rotate
// handle (above top-centre), plus the in-progress marquee rectangle. All in
// the already view-transformed canvas space; sizes divided by zoom so they
// stay constant on screen.
function _paintSelectionOverlay(ctx, s, v) {
  const accent = getComputedStyle(document.body).getPropertyValue('--accent').trim() || '#e8a838';
  // Marquee (selection drag box).
  if (s._marquee) {
    const m = s._marquee;
    const x = Math.min(m.x0, m.x1) * s.cssW, y = Math.min(m.y0, m.y1) * s.cssH;
    const w = Math.abs(m.x1 - m.x0) * s.cssW, h = Math.abs(m.y1 - m.y0) * s.cssH;
    ctx.save();
    ctx.strokeStyle = accent; ctx.fillStyle = accent;
    ctx.globalAlpha = 0.12; ctx.fillRect(x, y, w, h);
    ctx.globalAlpha = 0.9; ctx.lineWidth = 1 / v.zoom; ctx.setLineDash([4 / v.zoom, 3 / v.zoom]);
    ctx.strokeRect(x, y, w, h);
    ctx.restore();
  }
  if (!s._sel || !s._sel.length) return;
  const b = _selectionBounds(s._sel); if (!b) return;
  const x = b.x * s.cssW, y = b.y * s.cssH, w = b.w * s.cssW, h = b.h * s.cssH;
  const hs = 7 / v.zoom;  // handle half-size on screen
  ctx.save();
  ctx.strokeStyle = accent; ctx.fillStyle = accent;
  ctx.lineWidth = 1.5 / v.zoom; ctx.setLineDash([5 / v.zoom, 4 / v.zoom]);
  ctx.strokeRect(x, y, w, h);
  ctx.setLineDash([]);
  // Corner scale handles.
  const corners = [[x, y], [x + w, y], [x + w, y + h], [x, y + h]];
  for (const [cx, cy] of corners) { ctx.fillRect(cx - hs, cy - hs, hs * 2, hs * 2); }
  // Rotate handle: a small circle above the top-centre, with a connector.
  const rcx = x + w / 2, rcy = y - 24 / v.zoom;
  ctx.beginPath(); ctx.moveTo(x + w / 2, y); ctx.lineTo(rcx, rcy); ctx.stroke();
  ctx.beginPath(); ctx.arc(rcx, rcy, hs, 0, Math.PI * 2); ctx.fill();
  ctx.restore();
}

// Paint one stroke onto the live canvas, denormalising its points.
// Points are stored normalised: x in [0,1] vs width, y in [0,1] vs height.
function _paintStroke(ctx, st, cssW, cssH, aspect) {
  // Text element: draw the string at (x,y) top-left, font size in
  // normalised-height units so it scales with the canvas. Optional rot
  // (radians) rotates around the (x,y) anchor.
  if (st.t === 't') {
    if (!st.str || st._editing) return;
    const px = (st.fs || 0.04) * cssH;
    ctx.save();
    ctx.fillStyle = st.c || '#e8a838';
    ctx.font = `600 ${px}px 'Inter', system-ui, sans-serif`;
    ctx.textBaseline = 'top';
    const ax = st.x * cssW, ay = st.y * cssH;
    if (st.rot) { ctx.translate(ax, ay); ctx.rotate(st.rot); ctx.translate(-ax, -ay); }
    const lines = String(st.str).split('\n');
    let y = ay;
    for (const ln of lines) { ctx.fillText(ln, ax, y); y += px * 1.25; }
    ctx.restore();
    return;
  }
  if (!st.pts || !st.pts.length) return;
  ctx.strokeStyle = st.c || '#e8a838';
  ctx.lineCap = 'round';
  ctx.lineJoin = 'round';
  for (let i = 1; i < st.pts.length; i++) {
    const x0 = st.pts[i-1][0] * cssW, y0 = st.pts[i-1][1] * cssH;
    const x1 = st.pts[i][0]   * cssW, y1 = st.pts[i][1]   * cssH;
    const pr = st.pts[i][2] != null ? st.pts[i][2] : 0.5;
    ctx.lineWidth = (st.w || 5) * (0.5 + pr);
    ctx.beginPath();
    ctx.moveTo(x0, y0);
    ctx.lineTo(x1, y1);
    ctx.stroke();
  }
  // Single-point stroke (a tap) → draw a dot.
  if (st.pts.length === 1) {
    const x = st.pts[0][0] * cssW, y = st.pts[0][1] * cssH;
    const pr = st.pts[0][2] != null ? st.pts[0][2] : 0.5;
    ctx.fillStyle = st.c || '#e8a838';
    ctx.beginPath();
    ctx.arc(x, y, ((st.w || 5) * (0.5 + pr)) / 2, 0, Math.PI * 2);
    ctx.fill();
  }
}

function _bindNoteDrawPointer(canvas) {
  if (canvas._noteDrawBound) return;  // bind once per element
  canvas._noteDrawBound = true;

  // Track active pointers so we can detect pinch/two-finger pan.
  const active = new Map();  // pointerId -> {x, y}

  // Screen px relative to the canvas element.
  const screenPos = (e) => {
    const r = canvas.getBoundingClientRect();
    return { x: e.clientX - r.left, y: e.clientY - r.top };
  };
  // Stored normalised coordinate (accounts for pan/zoom), with pressure.
  const npos = (e) => {
    const s = _noteDrawState;
    const sp = screenPos(e);
    const [nx, ny] = _screenToNorm(s, sp.x, sp.y);
    return [nx, ny, (e.pressure && e.pressure > 0) ? e.pressure : 0.5];
  };

  // Begin a two-pointer pinch/pan gesture: cancel any in-progress stroke
  // (the first finger may have started one), and capture the initial
  // distance/midpoint in screen space + the view at gesture start.
  const beginGesture = () => {
    const s = _noteDrawState; if (!s) return;
    // Discard any stroke the first finger began — it wasn't meant as ink.
    if (s.cur) { s.cur = null; }
    s.drawing = false;
    s._activePointerId = null;
    // Pinch uses the two non-pen contacts so a stylus never warps the view.
    const pts = [...active.values()].filter(p => p.type !== 'pen');
    if (pts.length < 2) return;
    const dx = pts[1].x - pts[0].x, dy = pts[1].y - pts[0].y;
    s._gesture = {
      startDist: Math.hypot(dx, dy) || 1,
      startMidX: (pts[0].x + pts[1].x) / 2,
      startMidY: (pts[0].y + pts[1].y) / 2,
      startView: { ...s.view },
    };
    _redrawNoteStrokes();
  };

  const updateGesture = () => {
    const s = _noteDrawState; if (!s || !s._gesture) return;
    const pts = [...active.values()].filter(p => p.type !== 'pen');
    if (pts.length < 2) return;
    const g = s._gesture;
    const dx = pts[1].x - pts[0].x, dy = pts[1].y - pts[0].y;
    const dist = Math.hypot(dx, dy) || 1;
    const midX = (pts[0].x + pts[1].x) / 2;
    const midY = (pts[0].y + pts[1].y) / 2;
    const factor = dist / g.startDist;
    const newZoom = Math.max(0.25, Math.min(8, g.startView.zoom * factor));
    // Keep the gesture midpoint anchored in stored space while zooming, and
    // also translate by the midpoint movement (two-finger pan).
    const sv = g.startView;
    // Stored coord under the start midpoint:
    const anchorNx = (g.startMidX / s.cssW) / sv.zoom + sv.panX;
    const anchorNy = (g.startMidY / s.cssH) / sv.zoom + sv.panY;
    // After zoom, choose pan so that anchor maps to the *current* midpoint.
    s.view.zoom = newZoom;
    s.view.panX = anchorNx - (midX / s.cssW) / newZoom;
    s.view.panY = anchorNy - (midY / s.cssH) / newZoom;
    _clampView(s.view);
    _applyNoteDrawBg((notes.find(x => x.id === _editingNoteId) || {}).drawBg || 'none', s.view, s.cssW, s.cssH);
    _redrawNoteStrokes();
  };

  // One-pointer pan (hand mode).
  const beginPan = (e) => {
    const s = _noteDrawState; if (!s) return;
    const sp = screenPos(e);
    s._pan = { startX: sp.x, startY: sp.y, startView: { ...s.view } };
    canvas.classList.add('grabbing');
  };
  const updatePan = (e) => {
    const s = _noteDrawState; if (!s || !s._pan) return;
    const sp = screenPos(e);
    const dxN = (sp.x - s._pan.startX) / s.cssW / s.view.zoom;
    const dyN = (sp.y - s._pan.startY) / s.cssH / s.view.zoom;
    s.view.panX = s._pan.startView.panX - dxN;
    s.view.panY = s._pan.startView.panY - dyN;
    _clampView(s.view);
    _applyNoteDrawBg((notes.find(x => x.id === _editingNoteId) || {}).drawBg || 'none', s.view, s.cssW, s.cssH);
    _redrawNoteStrokes();
  };

  // ── Photo placement: drag to move, pinch to scale (operates on photoT) ──
  const _updatePhotoPanFn = (e) => {
    const s = _noteDrawState; if (!s || !s._photoPan) return;
    const sp = screenPos(e);
    // Convert screen delta to normalised, accounting for the view zoom so a
    // drag tracks the finger 1:1 on screen.
    const dxN = (sp.x - s._photoPan.startX) / s.cssW / s.view.zoom;
    const dyN = (sp.y - s._photoPan.startY) / s.cssH / s.view.zoom;
    s.photoT.x = s._photoPan.startT.x + dxN;
    s.photoT.y = s._photoPan.startT.y + dyN;
    _redrawNoteStrokes();
  };
  const _beginPhotoGesture = () => {
    const s = _noteDrawState; if (!s) return;
    s._photoPan = null;
    const pts = [...active.values()].filter(p => p.type !== 'pen');
    if (pts.length < 2) return;
    const dx = pts[1].x - pts[0].x, dy = pts[1].y - pts[0].y;
    s._photoGesture = {
      startDist: Math.hypot(dx, dy) || 1,
      startMidX: (pts[0].x + pts[1].x) / 2,
      startMidY: (pts[0].y + pts[1].y) / 2,
      startT: { ...s.photoT },
    };
  };
  const _updatePhotoGesture = () => {
    const s = _noteDrawState; if (!s || !s._photoGesture) return;
    const pts = [...active.values()].filter(p => p.type !== 'pen');
    if (pts.length < 2) return;
    const g = s._photoGesture;
    const dx = pts[1].x - pts[0].x, dy = pts[1].y - pts[0].y;
    const dist = Math.hypot(dx, dy) || 1;
    const factor = dist / g.startDist;
    const newScale = Math.max(0.05, Math.min(6, g.startT.scale * factor));
    // Keep the gesture midpoint anchored on the photo while scaling.
    const midX = (pts[0].x + pts[1].x) / 2, midY = (pts[0].y + pts[1].y) / 2;
    const midNxView = (g.startMidX / s.cssW) / s.view.zoom + s.view.panX;
    const midNyView = (g.startMidY / s.cssH) / s.view.zoom + s.view.panY;
    // Photo-local fraction of the midpoint at gesture start.
    const fx = (midNxView - g.startT.x) / Math.max(1e-6, g.startT.scale);
    const fy = (midNyView - g.startT.y) / Math.max(1e-6, (g.startT.scale / (s.photoNatAspect || 1)) * (s.cssW / s.cssH));
    const curMidNx = (midX / s.cssW) / s.view.zoom + s.view.panX;
    const curMidNy = (midY / s.cssH) / s.view.zoom + s.view.panY;
    s.photoT.scale = newScale;
    const newHN = (newScale / (s.photoNatAspect || 1)) * (s.cssW / s.cssH);
    s.photoT.x = curMidNx - fx * newScale;
    s.photoT.y = curMidNy - fy * newHN;
    _redrawNoteStrokes();
  };

  canvas.addEventListener('pointerdown', (e) => {
    const s = _noteDrawState; if (!s) return;
    e.preventDefault();
    canvas.setPointerCapture(e.pointerId);
    const sp = screenPos(e);
    active.set(e.pointerId, { x: sp.x, y: sp.y, type: e.pointerType });

    // ── Photo placement mode ── drags move the photo; two fingers scale it.
    if (s.placingPhoto && s.photoT) {
      if (active.size === 2) { _beginPhotoGesture(); return; }
      if (active.size > 2) return;
      s._photoPan = { startX: sp.x, startY: sp.y, startT: { ...s.photoT } };
      return;
    }

    // ── Transform tool ── select elements and scale/rotate/move them.
    if (s.tool === 'transform') {
      if (active.size > 1) return;  // ignore extra fingers during a transform
      const p = npos(e);
      const handle = _selHandleAt(s, s.view, sp.x, sp.y);
      if (handle && s._sel.length) {
        // Grabbed a handle (or inside the box) → begin a transform gesture.
        const b = _selectionBounds(s._sel);
        s._selXf = {
          mode: handle, pointerId: e.pointerId,
          startNx: p[0], startNy: p[1],
          box: b,
          cx: b.x + b.w / 2, cy: b.y + b.h / 2,
          snapshot: s._sel.map(i => _cloneStroke(s.strokes[i])),
          startAngle: Math.atan2(p[1] - (b.y + b.h / 2), p[0] - (b.x + b.w / 2)),
          moved: false,
        };
        return;
      }
      // No handle: tap an element to select it, or start a marquee.
      const hit = _elemAt(p[0], p[1]);
      if (hit >= 0) {
        s._sel = [hit];
        _redrawNoteStrokes();
      } else {
        s._marquee = { x0: p[0], y0: p[1], x1: p[0], y1: p[1], pointerId: e.pointerId };
      }
      return;
    }

    // Track stylus activity for palm rejection.
    if (e.pointerType === 'pen') {
      s.penDown++; s.lastPenAt = performance.now();
      // Palm-before-pen: if a touch had started a stroke or pan just before
      // the stylus landed, discard it — it was the palm, not intentional.
      if (s.drawing && s._activePointerId != null) {
        const owner = active.get(s._activePointerId);
        if (owner && owner.type === 'touch') {
          s.drawing = false; s.cur = null; s._activePointerId = null;
          _redrawNoteStrokes();
        }
      }
      if (s._pan) { s._pan = null; canvas.classList.remove('grabbing'); }
    }

    const stylusMode = _noteDrawStylusActive(s);
    const isPen = e.pointerType === 'pen';
    const isTouch = e.pointerType === 'touch';

    // ── Palm rejection (Auto) ──
    // In stylus mode, touch never inks. A palm landing while the pen is
    // actively drawing is ignored outright; otherwise a lone finger pans.
    if (stylusMode && isTouch) {
      // If a pen stroke is in progress, ignore the touch completely so a
      // resting palm can't disturb the view or the stroke.
      if (s.drawing && s.cur && !s.handMode) { return; }
      // Two touches → pinch/pan as usual.
      const touchCount = [...active.values()].filter(p => p.type === 'touch').length;
      if (touchCount >= 2) { beginGesture(); return; }
      // Single finger in stylus mode → pan the view (not ink).
      beginPan(e);
      return;
    }

    // ── Normal (no stylus active) ──
    // Two pointers down → pinch/pan gesture, regardless of tool. (When a pen
    // is the second pointer we don't pinch — pen is for drawing.)
    const ptList = [...active.values()];
    const nonPen = ptList.filter(p => p.type !== 'pen');
    if (!isPen && nonPen.length === 2 && active.size === 2) { beginGesture(); return; }
    if (active.size > 2) { return; }  // ignore extra fingers

    // One pointer (or a pen). Hand mode → pan. Otherwise draw/erase.
    if (s.handMode && !isPen) { beginPan(e); return; }

    const p = npos(e);
    // ── Text tool ── tap existing text to edit; tap+drag to move; tap empty
    // space to create. Drag is detected in pointermove via _textDrag.
    if (s.tool === 'text') {
      // If an editor is already open, a tap elsewhere commits it first.
      if (s._textEdit) { _commitNoteTextEdit(); }
      const hit = _textElemAt(p[0], p[1]);
      if (hit >= 0) {
        const el = s.strokes[hit];
        s._textDrag = {
          idx: hit, pointerId: e.pointerId,
          startNx: p[0], startNy: p[1],
          origX: el.x, origY: el.y, moved: false,
          preSnap: s.strokes.map(_cloneStroke),
        };
      } else {
        s._pendingTextAt = { x: p[0], y: p[1], pointerId: e.pointerId };
      }
      return;
    }
    if (s.tool === 'eraser') {
      s.drawing = true;
      s._activePointerId = e.pointerId;
      s._preGesture = s.strokes.map(_cloneStroke);  // for one-step undo
      s.erasedSomething = false;
      _eraseAt(p[0], p[1]);
      return;
    }
    s.drawing = true;
    s._activePointerId = e.pointerId;
    s.cur = { t: 'p', c: s.colour, w: s.size, pts: [p] };
    _redrawNoteStrokes();
  });

  canvas.addEventListener('pointermove', (e) => {
    const s = _noteDrawState; if (!s) return;
    if (active.has(e.pointerId)) {
      const sp = screenPos(e);
      active.set(e.pointerId, { x: sp.x, y: sp.y, type: e.pointerType });
    }
    if (e.pointerType === 'pen') s.lastPenAt = performance.now();

    // Photo placement gestures take priority while placing.
    if (s.placingPhoto) {
      if (s._photoGesture && active.size >= 2) { e.preventDefault(); _updatePhotoGesture(); return; }
      if (s._photoPan)                         { e.preventDefault(); _updatePhotoPanFn(e); return; }
      return;
    }

    // Transform tool: update marquee or apply move/scale/rotate to selection.
    if (s.tool === 'transform') {
      if (s._marquee && e.pointerId === s._marquee.pointerId) {
        e.preventDefault();
        const p = npos(e);
        s._marquee.x1 = p[0]; s._marquee.y1 = p[1];
        _redrawNoteStrokes();
        return;
      }
      if (s._selXf && e.pointerId === s._selXf.pointerId) {
        e.preventDefault();
        _applyNoteSelTransform(npos(e));
        return;
      }
      return;
    }

    // Text drag: move an existing text element under the pointer.
    if (s._textDrag && e.pointerId === s._textDrag.pointerId) {
      e.preventDefault();
      const p = npos(e);
      const el = s.strokes[s._textDrag.idx];
      if (el) {
        el.x = s._textDrag.origX + (p[0] - s._textDrag.startNx);
        el.y = s._textDrag.origY + (p[1] - s._textDrag.startNy);
        if (Math.abs(p[0] - s._textDrag.startNx) > 0.005 || Math.abs(p[1] - s._textDrag.startNy) > 0.005) s._textDrag.moved = true;
        _redrawNoteStrokes();
      }
      return;
    }

    // Active multi-touch gesture takes priority.
    if (s._gesture && active.size >= 2) { e.preventDefault(); updateGesture(); return; }
    if (s._pan)                         { e.preventDefault(); updatePan(e); return; }
    if (!s.drawing) return;
    // Only the pointer that started the stroke may extend it — a palm or
    // stray finger arriving mid-stroke is ignored.
    if (s._activePointerId != null && e.pointerId !== s._activePointerId) return;
    e.preventDefault();

    const events = (typeof e.getCoalescedEvents === 'function') ? e.getCoalescedEvents() : [e];
    const list = events.length ? events : [e];
    if (s.tool === 'eraser') {
      for (const ev of list) { const p = npos(ev); _eraseAt(p[0], p[1]); }
      return;
    }
    if (!s.cur) return;
    for (const ev of list) s.cur.pts.push(npos(ev));
    _redrawNoteStrokes();
  });

  const end = (e) => {
    const s = _noteDrawState; if (!s) return;
    active.delete(e.pointerId);
    try { canvas.releasePointerCapture(e.pointerId); } catch (_) {}

    if (e.pointerType === 'pen') { s.penDown = Math.max(0, s.penDown - 1); s.lastPenAt = performance.now(); }

    // Transform tool: finish a marquee selection or a transform gesture.
    if (s._marquee && e.pointerId === s._marquee.pointerId) {
      const m = s._marquee; s._marquee = null;
      const r = { x0: Math.min(m.x0, m.x1), y0: Math.min(m.y0, m.y1), x1: Math.max(m.x0, m.x1), y1: Math.max(m.y0, m.y1) };
      // A tiny marquee = a tap on empty space → clear selection.
      if (Math.abs(r.x1 - r.x0) < 0.01 && Math.abs(r.y1 - r.y0) < 0.01) {
        s._sel = [];
      } else {
        s._sel = [];
        for (let i = 0; i < s.strokes.length; i++) if (_elemInRect(s.strokes[i], r)) s._sel.push(i);
        if (s._sel.length) toast(`${s._sel.length} selected`);
      }
      _redrawNoteStrokes();
      return;
    }
    if (s._selXf && e.pointerId === s._selXf.pointerId) {
      const xf = s._selXf; s._selXf = null;
      if (xf.moved) {
        // Undo step: restore the pre-transform versions of the moved elements.
        const snap = s.strokes.map(_cloneStroke);
        // Replace the selected indices in the snapshot with their originals so
        // undo reverts only the transform.
        for (let k = 0; k < s._sel.length; k++) snap[s._sel[k]] = _cloneStroke(xf.snapshot[k]);
        s.history.push(snap);
        if (s.history.length > _NOTE_DRAW_MAX_HISTORY) s.history.shift();
        s.redo = [];
        _redrawNoteStrokes();
        _noteDrawCommit();
      }
      return;
    }

    // Text drag finished: if it moved, record an undo step; if it was a tap
    // (no movement), open the editor on that element instead.
    if (s._textDrag && e.pointerId === s._textDrag.pointerId) {
      const td = s._textDrag; s._textDrag = null;
      if (td.moved) {
        s.history.push(td.preSnap);
        if (s.history.length > _NOTE_DRAW_MAX_HISTORY) s.history.shift();
        s.redo = [];
        _noteDrawCommit();
      } else {
        _openNoteTextEditor(0, 0, td.idx);  // coords come from the element
      }
      return;
    }
    // Pending text create: a tap on empty space with the text tool.
    if (s._pendingTextAt && e.pointerId === s._pendingTextAt.pointerId) {
      const at = s._pendingTextAt; s._pendingTextAt = null;
      _openNoteTextEditor(at.x, at.y, null);
      return;
    }

    // Photo placement: end of a move/scale gesture.
    if (s.placingPhoto) {
      if (s._photoGesture && active.size < 2) {
        s._photoGesture = null;
        // If a finger is still down, let it continue as a drag.
        if (active.size === 1) {
          const sp = [...active.values()][0];
          s._photoPan = { startX: sp.x, startY: sp.y, startT: { ...s.photoT } };
        }
      }
      if (s._photoPan && active.size === 0) s._photoPan = null;
      // Persist placement as it settles (cheap; final lock-in on ✓).
      const n2 = notes.find(x => x.id === _editingNoteId);
      if (n2 && !n2.locked) { n2.drawPhotoT = s.photoT; }
      return;
    }

    // End of a pinch/pan gesture (a finger lifted).
    if (s._gesture) {
      if (active.size < 2) { s._gesture = null; }
      return;
    }
    if (s._pan) {
      if (active.size === 0) { s._pan = null; canvas.classList.remove('grabbing'); }
      return;
    }
    if (!s.drawing) return;
    // Only finish the stroke when the owning pointer lifts.
    if (s._activePointerId != null && e.pointerId !== s._activePointerId) return;
    s.drawing = false;
    s._activePointerId = null;
    if (s.tool === 'eraser') {
      if (s.erasedSomething && s._preGesture) {
        s.history.push(s._preGesture);
        if (s.history.length > _NOTE_DRAW_MAX_HISTORY) s.history.shift();
        s.redo = [];
      }
      s._preGesture = null;
      s.erasedSomething = false;
      _noteDrawCommit();
      return;
    }
    if (s.cur && s.cur.pts.length) {
      // Straightedge / shape snapping (input constraints — storage unchanged,
      // the result is still a normal point list).
      if (s.rulerMode && s.cur.pts.length >= 2) {
        // Ruler forces a straight line; no shape detection.
        s.cur.pts = _forceStraightLine(s.cur.pts);
      } else {
        // Always-on shape snap: clean up confident rectangles/ellipses/lines,
        // otherwise keep the freehand stroke (then simplify it as usual).
        const shape = _recognizeShape(s.cur.pts);
        if (shape) {
          s.cur.pts = shape.pts;
          const label = shape.kind === 'rectangle' ? 'rectangle'
            : shape.kind === 'ellipse' ? 'ellipse' : 'line';
          toast(`Snapped to ${label}`);
        } else {
          // Simplify the captured points before storing — a high-frequency
          // stylus emits far more points than needed; thinning them keeps the
          // vector format small and redraw cheap, with no visible quality loss.
          // Tolerance ÷ zoom so detail drawn while zoomed-in isn't flattened.
          s.cur.pts = _simplifyPoints(s.cur.pts, 0.0015 / (s.view ? s.view.zoom : 1));
        }
      }
      s.history.push(s.strokes.map(_cloneStroke));
      if (s.history.length > _NOTE_DRAW_MAX_HISTORY) s.history.shift();
      s.strokes.push(s.cur);
      s.redo = [];        // a new action invalidates redo
    }
    s.cur = null;
    _redrawNoteStrokes();
    _noteDrawCommit();
  };
  canvas.addEventListener('pointerup', end);
  canvas.addEventListener('pointercancel', end);
  canvas.addEventListener('pointerleave', end);

  // Mouse wheel / trackpad zoom (desktop), anchored at the cursor.
  canvas.addEventListener('wheel', (e) => {
    const s = _noteDrawState; if (!s) return;
    e.preventDefault();
    const sp = screenPos(e);
    const before = _screenToNorm(s, sp.x, sp.y);
    const factor = e.deltaY < 0 ? 1.1 : 1 / 1.1;
    s.view.zoom = Math.max(0.25, Math.min(8, s.view.zoom * factor));
    // Keep the cursor anchored over the same stored point.
    s.view.panX = before[0] - (sp.x / s.cssW) / s.view.zoom;
    s.view.panY = before[1] - (sp.y / s.cssH) / s.view.zoom;
    _clampView(s.view);
    _applyNoteDrawBg((notes.find(x => x.id === _editingNoteId) || {}).drawBg || 'none', s.view, s.cssW, s.cssH);
    _redrawNoteStrokes();
  }, { passive: false });
}

// Eraser: remove any stroke whose path passes near the point, using a hit
// radius scaled by the current brush size. The whole erase gesture is one
// undo step — the pre-gesture stroke list is snapshotted on pointerdown.
function _eraseAt(nx, ny) {
  const s = _noteDrawState; if (!s) return;
  const a = s.aspect || 1;
  // Hit radius is in stored-normalised units; divide by zoom so the eraser
  // feels the same size on screen regardless of magnification.
  const hitR = (s.size * 1.5) / Math.max(s.cssW, s.cssH) / (s.view ? s.view.zoom : 1);
  const before = s.strokes.length;
  s.strokes = s.strokes.filter(st => !_strokeNearPoint(st, nx, ny, hitR, a));
  if (s.strokes.length !== before) {
    s.erasedSomething = true;
    _redrawNoteStrokes();
  }
}

// Distance test: is point (nx,ny) within r of any segment of the stroke,
// or inside a text element's bounding box?
function _strokeNearPoint(st, nx, ny, r, aspect) {
  if (st.t === 't') {
    const b = _textElemBounds(st);
    if (!b) return false;
    // Inflate the box by the hit radius so it's easy to catch.
    return nx >= b.x - r && nx <= b.x + b.w + r && ny >= b.y - r && ny <= b.y + b.h + r;
  }
  const pts = st.pts; if (!pts || !pts.length) return false;
  if (pts.length === 1) {
    return _dist2(pts[0][0], pts[0][1], nx, ny) <= r * r;
  }
  for (let i = 1; i < pts.length; i++) {
    if (_distToSeg2(nx, ny, pts[i-1][0], pts[i-1][1], pts[i][0], pts[i][1]) <= r * r) return true;
  }
  return false;
}

// Measure a text element's bounding box in normalised space. Uses the live
// draw canvas's context for font metrics; falls back to a rough estimate.
let _noteTextMeasureCtx = null;
function _textElemBounds(st) {
  const s = _noteDrawState;
  // When measuring outside the editor (card/viewer/export) there's no live
  // canvas; use a neutral reference size — the returned values are
  // normalised, so only the ratio matters.
  const cssW = s ? s.cssW : 1000, cssH = s ? s.cssH : 1000;
  const px = (st.fs || 0.04) * cssH;
  if (!_noteTextMeasureCtx) {
    const c = document.createElement('canvas');
    _noteTextMeasureCtx = c.getContext('2d');
  }
  const mc = _noteTextMeasureCtx;
  mc.font = `600 ${px}px 'Inter', system-ui, sans-serif`;
  const lines = String(st.str || '').split('\n');
  let maxW = 0;
  for (const ln of lines) { const w = mc.measureText(ln).width; if (w > maxW) maxW = w; }
  const hPx = lines.length * px * 1.25;
  return { x: st.x, y: st.y, w: maxW / cssW, h: hPx / cssH };
}

// ── Selection & transform geometry ───────────────────────────────────
// Unified normalised bounding box {x,y,w,h} for any element (stroke or text).
function _elemBounds(st) {
  if (st.t === 't') return _textElemBounds(st);
  const pts = st.pts || [];
  if (!pts.length) return null;
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  for (const p of pts) {
    if (p[0] < minX) minX = p[0]; if (p[0] > maxX) maxX = p[0];
    if (p[1] < minY) minY = p[1]; if (p[1] > maxY) maxY = p[1];
  }
  return { x: minX, y: minY, w: maxX - minX, h: maxY - minY };
}

// Bounding box (normalised) enclosing the given element indices.
function _selectionBounds(indices) {
  const s = _noteDrawState; if (!s || !indices || !indices.length) return null;
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  for (const i of indices) {
    const b = _elemBounds(s.strokes[i]); if (!b) continue;
    if (b.x < minX) minX = b.x; if (b.y < minY) minY = b.y;
    if (b.x + b.w > maxX) maxX = b.x + b.w; if (b.y + b.h > maxY) maxY = b.y + b.h;
  }
  if (minX === Infinity) return null;
  return { x: minX, y: minY, w: maxX - minX, h: maxY - minY };
}

// Apply a point-mapping fn(x,y)->[x,y] to an element in place. Strokes have
// every point mapped. Text maps its anchor; scale is derived from how much
// the mapping stretches a unit step, and rotation is accumulated. `sc` is the
// uniform scale factor and `dRot` the rotation delta to fold into text.
function _transformElem(st, fn, sc, dRot) {
  if (st.t === 't') {
    const [nx, ny] = fn(st.x, st.y);
    st.x = nx; st.y = ny;
    if (sc && sc > 0) st.fs = Math.max(0.008, Math.min(0.5, (st.fs || 0.04) * sc));
    if (dRot) st.rot = (st.rot || 0) + dRot;
    return;
  }
  for (const p of st.pts) { const m = fn(p[0], p[1]); p[0] = m[0]; p[1] = m[1]; }
}

// Is a normalised point inside an element (for tap-select)? Reuses the
// eraser's segment/box test with a small tolerance.
function _pointInElem(st, nx, ny) {
  if (st.t === 't') {
    const b = _textElemBounds(st);
    return b && nx >= b.x && nx <= b.x + b.w && ny >= b.y && ny <= b.y + b.h;
  }
  return _strokeNearPoint(st, nx, ny, 0.02, 1);
}

// Does an element's bounding box intersect the marquee rect (for box-select)?
function _elemInRect(st, r) {
  const b = _elemBounds(st); if (!b) return false;
  return !(b.x > r.x1 || b.x + b.w < r.x0 || b.y > r.y1 || b.y + b.h < r.y0);
}

// Topmost element index under a point, or -1.
function _elemAt(nx, ny) {
  const s = _noteDrawState; if (!s) return -1;
  for (let i = s.strokes.length - 1; i >= 0; i--) {
    if (_pointInElem(s.strokes[i], nx, ny)) return i;
  }
  return -1;
}

// Apply the active transform gesture (move/scale corner/rotate) to the
// selected elements, rebuilding from the gesture snapshot each frame so the
// transform is absolute (no cumulative drift). p = current pointer [nx,ny].
function _applyNoteSelTransform(p) {
  const s = _noteDrawState, xf = s && s._selXf; if (!xf) return;
  const aspect = s.aspect || 1;
  let fn, sc = 1, dRot = 0;
  if (xf.mode === 'move') {
    const dx = p[0] - xf.startNx, dy = p[1] - xf.startNy;
    fn = (x, y) => [x + dx, y + dy];
  } else if (xf.mode === 'rotate') {
    const ang = Math.atan2(p[1] - xf.cy, p[0] - xf.cx);
    dRot = ang - xf.startAngle;
    const cos = Math.cos(dRot), sin = Math.sin(dRot);
    // Rotate about centre; correct for aspect so rotation looks circular on
    // screen (normalised x and y have different px scales).
    fn = (x, y) => {
      const rx = (x - xf.cx) * aspect, ry = (y - xf.cy);
      const nx = rx * cos - ry * sin, ny = rx * sin + ry * cos;
      return [xf.cx + nx / aspect, xf.cy + ny];
    };
  } else {
    // Corner scale: the opposite corner stays anchored; the dragged corner
    // tracks the pointer. Uniform scale = larger of the x/y ratios so the
    // selection keeps its proportions.
    const b = xf.box;
    const anchor = {
      nw: [b.x + b.w, b.y + b.h], ne: [b.x, b.y + b.h],
      se: [b.x, b.y], sw: [b.x + b.w, b.y],
    }[xf.mode];
    const w0 = Math.max(1e-4, b.w), h0 = Math.max(1e-4, b.h);
    const sx = Math.abs(p[0] - anchor[0]) / w0;
    const sy = Math.abs(p[1] - anchor[1]) / h0;
    sc = Math.max(0.05, Math.min(20, Math.max(sx, sy)));  // uniform
    fn = (x, y) => [anchor[0] + (x - anchor[0]) * sc, anchor[1] + (y - anchor[1]) * sc];
  }
  // Rebuild each selected element from its snapshot, then transform.
  for (let k = 0; k < s._sel.length; k++) {
    const idx = s._sel[k];
    const fresh = _cloneStroke(xf.snapshot[k]);
    _transformElem(fresh, fn, sc, dRot);
    s.strokes[idx] = fresh;
  }
  xf.moved = true;
  _redrawNoteStrokes();
}

function _dist2(x0, y0, x1, y1) { const dx = x1-x0, dy = y1-y0; return dx*dx + dy*dy; }
function _distToSeg2(px, py, ax, ay, bx, by) {
  const dx = bx-ax, dy = by-ay;
  const len2 = dx*dx + dy*dy;
  if (len2 === 0) return _dist2(px, py, ax, ay);
  let t = ((px-ax)*dx + (py-ay)*dy) / len2;
  t = Math.max(0, Math.min(1, t));
  return _dist2(px, py, ax + t*dx, ay + t*dy);
}

// Ramer–Douglas–Peucker point thinning. Drops points that sit within
// `tol` (normalised units) of the line between their kept neighbours.
// Keeps endpoints and pressure values on surviving points. Iterative
// (no recursion) so a long stroke can't overflow the stack.
function _simplifyPoints(pts, tol) {
  if (!pts || pts.length <= 2) return pts;
  const tol2 = tol * tol;
  const keep = new Uint8Array(pts.length);
  keep[0] = 1; keep[pts.length - 1] = 1;
  const stack = [[0, pts.length - 1]];
  while (stack.length) {
    const [first, last] = stack.pop();
    let maxD = -1, idx = -1;
    for (let i = first + 1; i < last; i++) {
      const d = _distToSeg2(pts[i][0], pts[i][1], pts[first][0], pts[first][1], pts[last][0], pts[last][1]);
      if (d > maxD) { maxD = d; idx = i; }
    }
    if (maxD > tol2 && idx !== -1) {
      keep[idx] = 1;
      stack.push([first, idx], [idx, last]);
    }
  }
  const out = [];
  for (let i = 0; i < pts.length; i++) if (keep[i]) out.push(pts[i]);
  return out;
}

// ── Straightedge & shape snapping ────────────────────────────────────
// Force a freehand point list into a straight line from first to last point.
// Keeps the average pressure so width feels consistent.
function _forceStraightLine(pts) {
  if (!pts || pts.length < 2) return pts;
  const a = pts[0], b = pts[pts.length - 1];
  const pr = (a[2] != null ? a[2] : 0.5);
  return [[a[0], a[1], pr], [b[0], b[1], pr]];
}

// Try to recognise a clean shape from a freehand stroke. Returns a new point
// array (line / rectangle / ellipse) on a confident match, or null to keep
// the freehand stroke. Conservative thresholds avoid intrusive false snaps
// since snapping is always-on. Works in normalised space.
function _recognizeShape(pts) {
  if (!pts || pts.length < 4) return null;
  // Bounding box and diagonal (scale reference).
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  for (const p of pts) {
    if (p[0] < minX) minX = p[0]; if (p[0] > maxX) maxX = p[0];
    if (p[1] < minY) minY = p[1]; if (p[1] > maxY) maxY = p[1];
  }
  const bw = maxX - minX, bh = maxY - minY;
  const diag = Math.hypot(bw, bh);
  if (diag < 0.04) return null;           // too small to be intentional
  const pr = (pts[0][2] != null ? pts[0][2] : 0.5);

  const start = pts[0], endp = pts[pts.length - 1];
  const gap = Math.hypot(endp[0] - start[0], endp[1] - start[1]);
  const closed = gap < diag * 0.22;       // endpoints meet → closed shape

  // ── Open shape: candidate straight line ──
  if (!closed) {
    // Max deviation of interior points from the start→end segment.
    let maxD2 = 0;
    for (let i = 1; i < pts.length - 1; i++) {
      const d2 = _distToSeg2(pts[i][0], pts[i][1], start[0], start[1], endp[0], endp[1]);
      if (d2 > maxD2) maxD2 = d2;
    }
    const dev = Math.sqrt(maxD2) / diag;
    if (dev < 0.06) {                      // hugs the straight line tightly
      return { kind: 'line', pts: [[start[0], start[1], pr], [endp[0], endp[1], pr]] };
    }
    return null;                           // open & curvy → leave freehand
  }

  // ── Closed shape: rectangle vs ellipse ──
  const cx = (minX + maxX) / 2, cy = (minY + maxY) / 2;
  const rx = bw / 2, ry = bh / 2;
  if (rx < 0.01 || ry < 0.01) return null; // degenerate

  // Ellipse fit: every point should sit near the inscribed ellipse, i.e.
  // ((x-cx)/rx)^2 + ((y-cy)/ry)^2 ≈ 1.
  let ellErr = 0;
  for (const p of pts) {
    const u = (p[0] - cx) / rx, v = (p[1] - cy) / ry;
    ellErr += Math.abs(Math.hypot(u, v) - 1);
  }
  ellErr /= pts.length;

  // Rectangle fit: every point should sit near the bounding-box perimeter
  // (distance to the nearest of the four edges, normalised by diag).
  let rectErr = 0;
  for (const p of pts) {
    const dl = Math.abs(p[0] - minX), dr = Math.abs(p[0] - maxX);
    const dt = Math.abs(p[1] - minY), db = Math.abs(p[1] - maxY);
    // distance to nearest edge line, but only counting the edge it's closest to
    const dxEdge = Math.min(dl, dr), dyEdge = Math.min(dt, db);
    rectErr += Math.min(dxEdge, dyEdge);
  }
  rectErr = (rectErr / pts.length) / diag;

  const ELL_OK = 0.10, RECT_OK = 0.035;
  const ellOk = ellErr < ELL_OK, rectOk = rectErr < RECT_OK;
  if (!ellOk && !rectOk) return null;

  // Prefer whichever fits better (normalise the two error scales roughly).
  const preferRect = rectOk && (!ellOk || (rectErr / RECT_OK) <= (ellErr / ELL_OK));
  if (preferRect) {
    return { kind: 'rectangle', pts: [
      [minX, minY, pr], [maxX, minY, pr], [maxX, maxY, pr], [minX, maxY, pr], [minX, minY, pr],
    ] };
  }
  // Ellipse: sample ~40 points.
  const ell = [];
  const N = 40;
  for (let i = 0; i <= N; i++) {
    const t = (i / N) * Math.PI * 2;
    ell.push([cx + rx * Math.cos(t), cy + ry * Math.sin(t), pr]);
  }
  return { kind: 'ellipse', pts: ell };
}


// True if the note has no drawable content.
function _noteDrawIsBlank() {
  const s = _noteDrawState; if (!s) return true;
  return !s.strokes.length;
}

// Stroke committed (or erase gesture ended) → persist + autosave.
function _noteDrawCommit() {
  const n = notes.find(x => x.id === _editingNoteId); if (!n) return;
  const s = _noteDrawState;
  _updateNoteDrawUndoRedoBtns();
  if (s) s.erasedSomething = false;
  if (!n.locked) {
    n.drawStrokes = s ? s.strokes.map(_cloneStroke) : [];
    n.drawAspect  = s ? s.aspect : 1;
    n.drawing = undefined;  // vector is now the source of truth; drop legacy PNG
  }
  _noteBodyDirty = true;
  clearTimeout(_noteAutoSaveTimer);
  _noteAutoSaveTimer = setTimeout(_autoSaveNote, 1000);
  if (n.locked) _resetNoteActivity(n.id);
}

function noteDrawUndo() {
  const s = _noteDrawState; if (!s || !s.history.length) return;
  s.redo.push(s.strokes.map(_cloneStroke));
  if (s.redo.length > _NOTE_DRAW_MAX_HISTORY) s.redo.shift();
  s.strokes = s.history.pop();
  s.cur = null;
  _redrawNoteStrokes();
  _noteDrawCommit();
}

function noteDrawRedo() {
  const s = _noteDrawState; if (!s || !s.redo.length) return;
  s.history.push(s.strokes.map(_cloneStroke));
  if (s.history.length > _NOTE_DRAW_MAX_HISTORY) s.history.shift();
  s.strokes = s.redo.pop();
  s.cur = null;
  _redrawNoteStrokes();
  _noteDrawCommit();
}

function noteDrawClear() {
  const s = _noteDrawState; if (!s) return;
  if (!s.strokes.length) return;
  if (!confirm('Clear this drawing?')) return;
  s.history.push(s.strokes.map(_cloneStroke));   // clear is one undo step
  if (s.history.length > _NOTE_DRAW_MAX_HISTORY) s.history.shift();
  s.redo = [];
  s.strokes = [];
  _redrawNoteStrokes();
  _noteDrawCommit();
}

function setNoteDrawTool(tool) {
  const s = _noteDrawState; if (!s) return;
  // Leaving the text tool while an inline editor is open → commit it first.
  if (s._textEdit && tool !== 'text') _commitNoteTextEdit();
  // Leaving the transform tool → clear any selection/overlay.
  if (s.tool === 'transform' && tool !== 'transform') {
    s._sel = []; s._selXf = null; s._marquee = null;
  }
  s.tool = tool;
  _updateNoteDrawToolUI();
  _updateNoteDrawTextControls();
  _redrawNoteStrokes();
}

// Text styling setters (separate from pen colour/size).
function setNoteTextColour(colour) {
  const s = _noteDrawState; if (!s) return;
  s.textColour = colour;
  if (s._textEdit) { const ta = document.getElementById('note-draw-text-input'); if (ta) ta.style.color = colour; }
  _updateNoteDrawTextControls();
}
function setNoteTextSize(fs) {
  const s = _noteDrawState; if (!s) return;
  s.textSize = fs;
  if (s._textEdit) { const ta = document.getElementById('note-draw-text-input'); if (ta) ta.style.fontSize = (fs * s.cssH * s.view.zoom) + 'px'; }
  _updateNoteDrawTextControls();
}

// Show the text colour/size controls only when the Text tool is active, and
// hide the pen size/colour controls (which don't apply to text) to reduce
// clutter. The transform tool also hides pen controls.
function _updateNoteDrawTextControls() {
  const s = _noteDrawState;
  const wrap = document.getElementById('note-draw-text-controls');
  const isText = !!(s && s.tool === 'text');
  if (wrap) wrap.style.display = isText ? 'flex' : 'none';
  // Pen size/colour are irrelevant for text & transform tools → hide them.
  const hidePen = !!(s && (s.tool === 'text' || s.tool === 'transform'));
  const flexEls = ['note-draw-sizes', 'note-draw-colours'];
  flexEls.forEach(id => { const el = document.getElementById(id); if (el) el.style.display = hidePen ? 'none' : 'flex'; });
  ['note-draw-div-pen1', 'note-draw-div-pen2'].forEach(id => { const el = document.getElementById(id); if (el) el.style.display = hidePen ? 'none' : ''; });
  if (!s) return;
  document.querySelectorAll('.note-draw-tsize').forEach(b => {
    b.classList.toggle('active', Math.abs(Number(b.dataset.tsize) - s.textSize) < 0.001);
  });
  const ti = document.getElementById('note-draw-text-colour');
  if (ti && /^#[0-9a-fA-F]{6}$/.test(s.textColour || '')) ti.value = s.textColour;
}

function setNoteDrawColour(colour) {
  const s = _noteDrawState; if (!s) return;
  s.colour = colour;
  if (s.tool === 'eraser') s.tool = 'pen';  // picking a colour implies pen
  _updateNoteDrawToolUI();
}

function setNoteDrawSize(size) {
  const s = _noteDrawState; if (!s) return;
  s.size = size;
  _updateNoteDrawToolUI();
}

function _updateNoteDrawToolUI() {
  const s = _noteDrawState; if (!s) return;
  document.getElementById('note-draw-pen')?.classList.toggle('active', s.tool === 'pen');
  document.getElementById('note-draw-eraser')?.classList.toggle('active', s.tool === 'eraser');
  document.getElementById('note-draw-text')?.classList.toggle('active', s.tool === 'text');
  document.getElementById('note-draw-transform')?.classList.toggle('active', s.tool === 'transform');
  document.querySelectorAll('.note-draw-size').forEach(b => {
    b.classList.toggle('active', Number(b.dataset.size) === s.size);
  });
  const presets = ['#e8a838', '#ffffff', '#111111', '#e05c5c', '#5cc77d', '#5c9fe0'];
  document.querySelectorAll('.note-draw-swatch').forEach(b => {
    if (b.classList.contains('note-draw-custom')) return;
    b.classList.toggle('active', b.dataset.dcolour === s.colour && s.tool === 'pen');
  });
  // Custom colour: reflect the chosen value into the picker, and mark it
  // active when the current colour isn't one of the presets.
  const custom = document.getElementById('note-draw-custom-wrap');
  const customInput = document.getElementById('note-draw-custom');
  const isCustom = s.tool === 'pen' && !presets.includes((s.colour || '').toLowerCase());
  if (custom) custom.classList.toggle('active', isCustom);
  if (customInput && /^#[0-9a-fA-F]{6}$/.test(s.colour || '')) customInput.value = s.colour;
}

// ── Text element editing ─────────────────────────────────────────────
// Open the inline text editor at a normalised point. If editingIdx is given,
// edit that existing text element; otherwise create a new one on commit.
function _openNoteTextEditor(nx, ny, editingIdx) {
  const s = _noteDrawState; if (!s) return;
  const host = document.getElementById('note-draw-canvas-host');
  if (!host) return;
  // If editing an existing element, seed from it; else start blank.
  let initial = '';
  if (editingIdx != null && s.strokes[editingIdx] && s.strokes[editingIdx].t === 't') {
    const el = s.strokes[editingIdx];
    initial = el.str || '';
    nx = el.x; ny = el.y;
    s.textColour = el.c || s.textColour;
    s.textSize = el.fs || s.textSize;
    // Hide the element while editing so it isn't drawn twice (canvas + box).
    el._editing = true;
    _redrawNoteStrokes();
  }
  s._textEdit = { x: nx, y: ny, idx: (editingIdx != null ? editingIdx : null) };

  let ta = document.getElementById('note-draw-text-input');
  if (!ta) {
    ta = document.createElement('textarea');
    ta.id = 'note-draw-text-input';
    ta.setAttribute('rows', '1');
    ta.style.cssText = 'position:absolute;z-index:5;margin:0;padding:0;border:0;outline:1px dashed var(--accent);background:rgba(0,0,0,0.35);resize:none;overflow:hidden;line-height:1.25;font-family:var(--sans),system-ui,sans-serif;font-weight:600;white-space:pre;min-width:24px;box-sizing:content-box';
    host.appendChild(ta);
    ta.addEventListener('blur', () => _commitNoteTextEdit());
    ta.addEventListener('keydown', (ev) => {
      // Enter commits (Shift+Enter for newline); Esc cancels.
      if (ev.key === 'Enter' && !ev.shiftKey) { ev.preventDefault(); ta.blur(); }
      else if (ev.key === 'Escape') { ev.preventDefault(); _cancelNoteTextEdit(); }
    });
    ta.addEventListener('input', () => _sizeNoteTextInput(ta));
  }
  // Position & style the box at the normalised point under the view.
  const fontPx = s.textSize * s.cssH * s.view.zoom;
  const left = (nx - s.view.panX) * s.view.zoom * s.cssW;
  // Canvas paints with textBaseline 'top'; the textarea's line-height:1.25
  // adds half-leading (0.125em) above the first line, so shift the box up by
  // that amount so the visible glyphs line up with where they'll be painted.
  const top  = (ny - s.view.panY) * s.view.zoom * s.cssH - fontPx * 0.125;
  ta.style.left = left.toFixed(1) + 'px';
  ta.style.top  = top.toFixed(1) + 'px';
  ta.style.color = s.textColour;
  ta.style.fontSize = fontPx.toFixed(1) + 'px';
  ta.style.display = 'block';
  ta.value = initial;
  _sizeNoteTextInput(ta);
  setTimeout(() => { ta.focus(); ta.setSelectionRange(ta.value.length, ta.value.length); }, 0);
}

function _sizeNoteTextInput(ta) {
  ta.style.width = 'auto'; ta.style.height = 'auto';
  ta.style.width = Math.max(24, ta.scrollWidth + 6) + 'px';
  ta.style.height = ta.scrollHeight + 'px';
}

function _commitNoteTextEdit() {
  const s = _noteDrawState; if (!s || !s._textEdit) return;
  const ta = document.getElementById('note-draw-text-input');
  const edit = s._textEdit;
  s._textEdit = null;
  const str = ta ? ta.value.replace(/\s+$/,'') : '';
  if (ta) ta.style.display = 'none';

  // Snapshot for undo before mutating.
  const snap = s.strokes.map(_cloneStroke);

  if (edit.idx != null && s.strokes[edit.idx]) {
    const el = s.strokes[edit.idx];
    delete el._editing;
    if (!str.trim()) {
      // Emptied → delete the element.
      s.strokes.splice(edit.idx, 1);
    } else {
      el.str = str; el.c = s.textColour; el.fs = s.textSize;
    }
    s.history.push(snap);
    if (s.history.length > _NOTE_DRAW_MAX_HISTORY) s.history.shift();
    s.redo = [];
  } else if (str.trim()) {
    s.strokes.push({ t: 't', x: edit.x, y: edit.y, str, fs: s.textSize, c: s.textColour });
    s.history.push(snap);
    if (s.history.length > _NOTE_DRAW_MAX_HISTORY) s.history.shift();
    s.redo = [];
  }
  _redrawNoteStrokes();
  _noteDrawCommit();
}

function _cancelNoteTextEdit() {
  const s = _noteDrawState; if (!s) return;
  const ta = document.getElementById('note-draw-text-input');
  if (ta) ta.style.display = 'none';
  if (s._textEdit && s._textEdit.idx != null && s.strokes[s._textEdit.idx]) {
    delete s.strokes[s._textEdit.idx]._editing;
  }
  s._textEdit = null;
  _redrawNoteStrokes();
}

// Find the topmost text element under a normalised point, or -1.
function _textElemAt(nx, ny) {
  const s = _noteDrawState; if (!s) return -1;
  for (let i = s.strokes.length - 1; i >= 0; i--) {
    const st = s.strokes[i];
    if (st.t !== 't') continue;
    const b = _textElemBounds(st);
    if (b && nx >= b.x && nx <= b.x + b.w && ny >= b.y && ny <= b.y + b.h) return i;
  }
  return -1;
}

function _updateNoteDrawUndoRedoBtns() {
  const s = _noteDrawState;
  const u = document.getElementById('note-draw-undo');
  const r = document.getElementById('note-draw-redo');
  const canU = !!(s && s.history.length);
  const canR = !!(s && s.redo.length);
  if (u) { u.disabled = !canU; u.style.opacity = canU ? '' : '0.4'; }
  if (r) { r.disabled = !canR; r.style.opacity = canR ? '' : '0.4'; }
}

async function toggleNoteDraw() {
  const n = notes.find(x => x.id === _editingNoteId); if (!n) return;

  if (!n.drawMode) {
    // Switching INTO draw mode. If the note already has typed text, warn —
    // draw and text are mutually exclusive, and we don't discard the text
    // (it's preserved in n.body and restored if they toggle back off), but
    // it won't be visible while drawing.
    const hasText = n.tickBoxesVisible
      ? !!(n.tickItems && n.tickItems.length)
      : !!(document.getElementById('note-body-input')?.innerText || '').trim();
    if (hasText && !confirm('Switch to drawing mode? Your typed text is kept but hidden while you draw — toggle back to see it.')) {
      return;
    }
    if (_noteBodyDirty) { try { await _autoSaveNote(); } catch (_) {} }
    n.drawMode = true;
    if (n.tickBoxesVisible) {
      n.tickBoxesVisible = false;
      document.getElementById('note-btn-tick')?.classList.toggle('active', false);
      const ticksEl = document.getElementById('note-ticks-body');
      if (ticksEl) ticksEl.style.display = 'none';
    }
  } else {
    if (_noteDrawState && _noteDrawState._textEdit) _commitNoteTextEdit();
    n.drawMode = false;
    _noteDrawState = null;
  }

  document.getElementById('note-btn-draw')?.classList.toggle('active', n.drawMode);
  _showNoteBody(n);
  n.updatedAt = new Date().toISOString();
  await saveNotes();
  _noteBodyDirty = true;
  clearTimeout(_noteAutoSaveTimer);
  _noteAutoSaveTimer = setTimeout(_autoSaveNote, 600);
}

function _getCurrentEditorBody(n) {
  if (n.drawMode) {
    // In draw mode the "body" carries the vector strokes as JSON wrapped in
    // a sentinel, so locked-note sync (body string only) round-trips them.
    // For unlocked notes we also mirror onto n.drawStrokes/n.drawAspect so
    // cards/viewer can render without decrypting.
    const s = (_noteDrawState && _noteDrawState.noteId === n.id) ? _noteDrawState : null;
    const strokes = s ? s.strokes : (Array.isArray(n.drawStrokes) ? n.drawStrokes : []);
    const aspect  = s ? s.aspect  : (n.drawAspect || 1);
    const photo   = s ? (s.photoData || '') : (n.drawPhoto || '');
    const photoT  = s ? (s.photoT || null)  : (n.drawPhotoT || null);
    if (!n.locked) {
      n.drawStrokes = strokes.map(_cloneStroke);
      n.drawAspect  = aspect;
      n.drawPhoto   = photo || null;
      n.drawPhotoT  = photo ? photoT : null;
      n.drawing = undefined;
    }
    if (!strokes.length && !photo) return '';
    return _serializeVDraw(strokes, aspect, photo, photoT);
  }
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
  let   title   = (titleEl?.value || '').trim();
  // Drawings often have no title. Rather than silently dropping the
  // artwork (the title gate below would return early), give an untitled
  // drawing a sensible default so the canvas data persists and syncs.
  if (!title && n.drawMode) {
    title = 'Drawing';
    if (titleEl) titleEl.value = title;
  }
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
