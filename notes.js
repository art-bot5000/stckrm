// ═══════════════════════════════════════════════════════════════════
//  NOTES CORE (always-loaded half of the Notes feature)
// ═══════════════════════════════════════════════════════════════════
//
// This file is the ALWAYS-LOADED core of Secure Notes. It contains only
// what runs on the boot / sync / tab-blur / omnibox hot paths: load + save,
// the note-id helper, the secure re-lock + inactivity timer (which fire on
// visibilitychange app-wide), the theme bg helper, the sync-on-change hook,
// and the editor-session state those touch. It is loaded eagerly via
//   <script src="notes.js" defer>
// in index.html, exactly as the old single-file notes.js was.
//
// The Notes VIEW (renderNotes, the note cards, the full editor, sharing
// panel, lock screens, tick-boxes, colour picker, undo/redo, multi-select
// action bar, linked-reminder UI) lives in notes-ui.js, LAZY-LOADED on
// first open of the Notes view via window._loadNotesUI() (see index.html,
// same pattern as _loadScanner / _loadDemo / _loadBudgetUI). app.js awaits
// _loadNotesUI() inside showView('notes') before calling renderNotes().
//
// Two calls here reach into notes-ui.js (renderNotes, in _relockAllNotes and
// the inactivity timer) and are guarded with `typeof renderNotes ===
// 'function'` — if the Notes view isn't open there is nothing to re-render.
// The note STATE app.js reads directly (the `notes` array, `_noteUnlocked`)
// lives HERE so the sync path never depends on the lazy bundle.
//
// Load order in index.html MUST be: app.js FIRST, then notes.js.
// notes.js (core), notes-ui.js, app.js and index.html must all land in
// GitHub together for this split to work.
//
// Outbound deps on app.js (globals): toast, esc, postKV, dbGet, dbPut,
// WORKER_URL, _kvSessionToken, _kvEmailHash, kvConnected, _shareState, etc.
// ═══════════════════════════════════════════════════════════════════

// One-time load-order sanity check. notes.js depends on globals defined
// in app.js (loaded immediately before us with `defer`). If any are missing
// it means the script ordering in index.html has been changed.
(function _notesLoadOrderCheck() {
  const required = ['toast', 'esc', 'postKV', 'dbGet', 'dbPut'];
  const missing  = required.filter(name => typeof window[name] === 'undefined');
  if (missing.length > 0) {
    console.error('[notes.js] Missing globals at load time:', missing,
      '— check script order in index.html');
  }
})();

// ── State ─────────────────────────────────────────────────────────
let notes = [];                    // array of note metadata + body (unlocked) or no body (locked)
let _editingNoteId = null;         // currently open note id
let _noteUnlocked = new Map();     // noteId → { body, lastActivity, inactivityTimer }
let _noteColourPickerOpen = false;
let _noteBodyDirty = false;        // unsaved changes flag
let _noteAutoSaveTimer = null;
async function loadNotes() {
  const stored = await dbGet('items', 'notes');
  if (stored && Array.isArray(stored)) notes = stored;
}

async function saveNotes() {
  await dbPut('items', 'notes', notes);
}

// ── Theme-aware colour mapping ───────────────────────────────────
const _NOTE_COLOUR_LIGHT_MAP = {
  '#3a2e10': '#fbeccc', // amber
  '#3a1010': '#fbd6d6', // red
  '#0e3020': '#d3ecdd', // green
  '#0e2040': '#d6e2f5', // blue
  '#2a1040': '#e5d8f0', // purple
  '#0e3030': '#d3ecec', // teal
};
function _noteBgForCurrentTheme(rawColour) {
  const isLight = document.documentElement.getAttribute('data-theme') === 'light';
  if (!rawColour) {
    return isLight ? 'var(--surface)' : 'var(--bg)';
  }
  if (isLight && _NOTE_COLOUR_LIGHT_MAP[rawColour]) {
    return _NOTE_COLOUR_LIGHT_MAP[rawColour];
  }
  return rawColour;
}

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
      // renderNotes lives in the lazy notes-ui.js. If the Notes view isn't
      // open the bundle may not be loaded; nothing to re-render, so guard.
      if (typeof renderNotes === 'function') renderNotes();
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
      if (typeof renderNotes === 'function') renderNotes();
      if (typeof toast === 'function') toast('Note locked after inactivity');
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
function _closeNoteEditorImmediate() {
  const overlay = document.getElementById('note-editor-overlay');
  if (overlay) overlay.style.display = 'none';
  // Remove the body class set by openNoteEditor so the FAB returns and
  // background scrolling resumes. Safe to call even if class isn't set.
  document.body.classList.remove('note-open');
  _editingNoteId = null;
  _noteBodyDirty = false;
  clearTimeout(_noteAutoSaveTimer);
  _noteColourPickerOpen = false;
  const picker = document.getElementById('note-colour-picker');
  if (picker) picker.style.display = 'none';
}

async function _syncNoteIfConnected() {
  if (kvConnected && !_shareState) {
    kvPush().catch(e => console.warn('notes kvPush:', e.message));
  }
}

// ═══════════════════════════════════════════════════════════════════
//  PRE-LOAD SHIMS for static Notes-view buttons
// ═══════════════════════════════════════════════════════════════════
// The Notes view chrome in index.html (the "+ New note" button, the
// All/Pinned/Archived/Trash filter chips, "Empty trash") has inline
// onclick handlers that call openNoteEditor / setNotesFilter /
// emptyNotesTrash by name. Those real functions live in the lazy
// notes-ui.js. showView('notes') reveals that chrome and kicks off
// _loadNotesUI() asynchronously, so for the brief window before the
// bundle resolves (first open only — cached afterwards) a fast tap would
// hit an undefined function. These shims close that gap: load the UI,
// then dispatch to the real function. notes-ui.js declares real
// `function` versions of these names which hoist over these shims when it
// loads, so after first open the shims are gone and never run again.
(function _installNotesUIShims() {
  ['openNoteEditor', 'setNotesFilter', 'emptyNotesTrash'].forEach(function (name) {
    if (typeof window[name] === 'function') return;   // real impl already present
    var shim = function () {
      var args = arguments;
      if (typeof window._loadNotesUI !== 'function') return;
      window._loadNotesUI().then(function () {
        // After load, window[name] is the real notes-ui.js function (it
        // hoisted over this shim). Dispatch only if it's no longer us.
        if (typeof window[name] === 'function' && window[name] !== shim) {
          window[name].apply(null, args);
        }
      }).catch(function (err) {
        console.error('notes-ui.js failed to load:', err);
        if (typeof toast === 'function') toast('Notes unavailable \u2014 please reload the page');
      });
    };
    window[name] = shim;
  });
})();

