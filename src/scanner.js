
// ═══════════════════════════════════════════
//  BARCODE SCANNER
// ═══════════════════════════════════════════
let barcodeStream   = null;
let barcodeInterval = null;

async function openBarcodeScanner() {
  // Check for BarcodeDetector API support
  if (!('BarcodeDetector' in window)) {
    toast('Barcode scanning not supported on this browser — try Chrome on Android');
    return;
  }
  openModal('barcode-modal');
  const video     = document.getElementById('barcode-video');
  const statusEl  = document.getElementById('barcode-status');
  try {
    barcodeStream = await navigator.mediaDevices.getUserMedia({ video: { facingMode: 'environment' } });
    video.srcObject = barcodeStream;
    const detector = new BarcodeDetector({ formats: ['ean_13','ean_8','upc_a','upc_e','code_128','code_39'] });
    barcodeInterval = setInterval(async () => {
      if (video.readyState < 2) return;
      try {
        const codes = await detector.detect(video);
        if (codes.length) {
          clearInterval(barcodeInterval);
          const barcode = codes[0].rawValue;
          statusEl.textContent = `Found: ${barcode} — looking up product…`;
          navigator.vibrate && navigator.vibrate([50, 30, 50]);
          await lookupBarcode(barcode);
        }
      } catch(e) {}
    }, 400);
  } catch(e) {
    statusEl.textContent = 'Could not access camera. Please check permissions.';
  }
}

function closeBarcodeScanner() {
  clearInterval(barcodeInterval);
  barcodeInterval = null;
  if (barcodeStream) { barcodeStream.getTracks().forEach(t => t.stop()); barcodeStream = null; }
  closeModal('barcode-modal');
}

async function lookupBarcode(barcode) {
  const statusEl = document.getElementById('barcode-status');
  const isQuickAdd   = sessionStorage.getItem('barcode_target') === 'quick-add';
  const isScanChooser = sessionStorage.getItem('barcode_target') === 'scan-chooser';

  try {
    let productName = null;
    let imageUrl    = null;

    // Try Open Food Facts first
    let res  = await fetch(`https://world.openfoodfacts.org/api/v0/product/${barcode}.json`);
    let data = await res.json();
    if (data.status === 1 && data.product?.product_name) {
      productName = data.product.product_name;
      imageUrl    = data.product.image_url || null;
    }

    // Try Open Beauty Facts
    if (!productName) {
      res  = await fetch(`https://world.openbeautyfacts.org/api/v0/product/${barcode}.json`);
      data = await res.json();
      if (data.status === 1 && data.product?.product_name) {
        productName = data.product.product_name;
        imageUrl    = data.product.image_url || null;
      }
    }

    closeBarcodeScanner();
    sessionStorage.removeItem('barcode_target');

    if (productName) {
      if (isScanChooser) {
        // Ask what to do with the scanned item
        openScanChooser(productName, imageUrl);
      } else if (isQuickAdd) {
        // Append to quick-add textarea
        const ta  = document.getElementById('quick-add-input');
        const cur = ta.value.trim();
        ta.value  = cur ? cur + ', ' + productName : productName;
        updateQuickAddPreview();
        toast(`Found: ${productName}`);
        openModal('quick-add-modal');
      } else {
        document.getElementById('f-name').value     = productName;
        document.getElementById('f-category').value = 'Food & Drink';
        if (imageUrl) { pendingImageUrl = imageUrl; showImagePreview(imageUrl, 'Image found via barcode'); }
        toast('Product found ✓');
      }
    } else {
      if (isScanChooser) {
        openScanChooser(barcode, null);
      } else if (isQuickAdd) {
        // Use barcode as name fallback
        const ta  = document.getElementById('quick-add-input');
        const cur = ta.value.trim();
        ta.value  = cur ? cur + ', ' + barcode : barcode;
        updateQuickAddPreview();
        toast(`Barcode ${barcode} added — rename it later`);
        openModal('quick-add-modal');
      } else {
        document.getElementById('f-name').focus();
        toast(`Barcode not found — enter name manually`);
      }
    }
  } catch(e) {
    sessionStorage.removeItem('barcode_target');
    closeBarcodeScanner();
    toast('Lookup failed — check your connection');
  }
}

// ═══════════════════════════════════════════
//  STORE PRICES (multiple stores per item)
// ═══════════════════════════════════════════
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
        style="flex:1;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:5px 8px;color:var(--text);font-size:12px;font-family:var(--sans)"
        onchange="updateStorePrice(${i},'store',this.value)">
      <input type="text" value="${esc(sp.price)}" placeholder="Price"
        style="width:80px;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:5px 8px;color:var(--text);font-size:12px;font-family:var(--mono)"
        onchange="updateStorePrice(${i},'price',this.value)">
      <button onclick="removeStorePrice(${i})" style="background:none;border:none;cursor:pointer;color:var(--muted);font-size:16px;padding:2px 4px"
        onmouseover="this.style.color='var(--danger)'" onmouseout="this.style.color='var(--muted)'">✕</button>
    </div>`).join('');

  // Show cheapest badge if multiple prices
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

// Temporary store prices for the current modal session
let tempStorePrices = [];

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
        style="flex:1;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:5px 8px;color:var(--text);font-size:12px;font-family:var(--sans)"
        oninput="updateStorePrice(${i},'store',this.value)">
      <input type="text" value="${esc(sp.price)}" placeholder="e.g. £12.99"
        style="width:90px;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:5px 8px;color:var(--text);font-size:12px;font-family:var(--mono)"
        oninput="updateStorePrice(${i},'price',this.value)">
      <button onclick="removeStorePrice(${i})" style="background:none;border:none;cursor:pointer;color:var(--muted);font-size:16px;padding:2px 4px"
        onmouseover="this.style.color='var(--danger)'" onmouseout="this.style.color='var(--muted)'">✕</button>
    </div>`).join('');
}


// ═══════════════════════════════════════════
//  SHARE ITEM
// ═══════════════════════════════════════════
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
  canvas.width  = W;
  canvas.height = H;
  const ctx = canvas.getContext('2d');

  // Background gradient
  const grad = ctx.createLinearGradient(0, 0, W, H);
  grad.addColorStop(0, '#0f1117');
  grad.addColorStop(1, '#1a1d27');
  ctx.fillStyle = grad;
  ctx.fillRect(0, 0, W, H);

  // Subtle amber glow top-left
  const glow = ctx.createRadialGradient(0, 0, 0, 0, 0, 260);
  glow.addColorStop(0, 'rgba(232,168,56,0.07)');
  glow.addColorStop(1, 'rgba(232,168,56,0)');
  ctx.fillStyle = glow;
  ctx.fillRect(0, 0, W, H);

  // Border
  ctx.strokeStyle = '#2e3350';
  ctx.lineWidth = 1;
  ctx.strokeRect(0.5, 0.5, W - 1, H - 1);

  // ── App branding header ──
  const headerH = 44;
  ctx.fillStyle = '#1a1d27';
  ctx.fillRect(0, 0, W, headerH);
  ctx.strokeStyle = '#2e3350';
  ctx.lineWidth = 1;
  ctx.beginPath(); ctx.moveTo(0, headerH); ctx.lineTo(W, headerH); ctx.stroke();

  // Logo mark
  ctx.fillStyle = '#e8a838';
  ctx.font = 'bold 13px monospace';
  ctx.fillText('📦 STOCKROOM', 20, 28);

  // Tagline right
  ctx.fillStyle = '#7880a0';
  ctx.font = '11px monospace';
  ctx.textAlign = 'right';
  ctx.fillText('Household Consumables Tracker', W - 20, 28);
  ctx.textAlign = 'left';

  // ── Product content ──
  const contentY = headerH + 24;

  // Category chip
  ctx.fillStyle = 'rgba(120,128,160,0.2)';
  const catText = (item.category || 'Other').toUpperCase();
  ctx.font = '10px monospace';
  const catW = ctx.measureText(catText).width + 16;
  roundRect(ctx, 20, contentY, catW, 20, 4);
  ctx.fill();
  ctx.fillStyle = '#7880a0';
  ctx.fillText(catText, 28, contentY + 14);

  // Product name — large, wraps up to 2 lines
  ctx.fillStyle = '#e8eaf2';
  ctx.font = 'bold 28px system-ui, sans-serif';
  const nameLines = wrapText(ctx, item.name || '', W - 48);
  nameLines.slice(0, 2).forEach((line, i) => {
    ctx.fillText(line, 20, contentY + 46 + i * 36);
  });
  const afterName = contentY + 46 + Math.min(nameLines.length, 2) * 36;

  // ── Info pills row ──
  const pillY = afterName + 16;
  let pillX = 20;

  const drawPill = (label, value, bg, textCol) => {
    if (!value) return 0;
    ctx.font = '11px system-ui, sans-serif';
    const fullText = `${label}: ${value}`;
    const pw = ctx.measureText(fullText).width + 20;
    ctx.fillStyle = bg;
    ctx.beginPath();
    ctx.roundRect(pillX, pillY, pw, 24, 12);
    ctx.fill();
    ctx.fillStyle = textCol;
    ctx.fillText(fullText, pillX + 10, pillY + 16);
    const used = pw + 8;
    pillX += used;
    return used;
  };

  // Last price
  const history   = getPriceHistory(item);
  const lastPrice = history.length
    ? (history[history.length - 1].raw || `£${history[history.length-1].price.toFixed(2)}`)
    : null;
  if (lastPrice) drawPill('Price', lastPrice, 'rgba(76,187,138,0.15)', '#4cbb8a');

  // Store
  if (item.store) drawPill('From', item.store, 'rgba(91,141,238,0.15)', '#5b8dee');

  // Qty
  if (item.qty && item.qty !== 1) drawPill('Qty', `×${item.qty}`, 'rgba(120,128,160,0.15)', '#7880a0');

  // Rating
  if (item.rating) {
    const stars = '★'.repeat(item.rating) + '☆'.repeat(5 - item.rating);
    drawPill('', stars, 'rgba(232,168,56,0.15)', '#e8a838');
  }

  // ── Store prices comparison ──
  const storePrices = (item.storePrices || []).filter(sp => sp.store && sp.price);
  if (storePrices.length > 1) {
    const priceY = pillY + 40;
    ctx.fillStyle = '#7880a0';
    ctx.font = '10px monospace';
    ctx.fillText('PRICE COMPARISON', 20, priceY);
    let spX = 20;
    storePrices.slice(0, 4).forEach(sp => {
      ctx.font = '11px system-ui, sans-serif';
      const txt = `${sp.store}: ${sp.price}`;
      const pw  = ctx.measureText(txt).width + 16;
      ctx.fillStyle = 'rgba(46,51,80,0.8)';
      ctx.beginPath();
      ctx.roundRect(spX, priceY + 8, pw, 22, 6);
      ctx.fill();
      ctx.fillStyle = '#e8eaf2';
      ctx.fillText(txt, spX + 8, priceY + 23);
      spX += pw + 6;
    });
  }

  // ── Notes ──
  if (item.notes) {
    const notesY = H - 60;
    ctx.fillStyle = 'rgba(120,128,160,0.12)';
    ctx.fillRect(20, notesY, W - 40, 26);
    ctx.fillStyle = '#7880a0';
    ctx.font = 'italic 12px system-ui, sans-serif';
    const noteTrunc = item.notes.length > 60 ? item.notes.slice(0, 57) + '…' : item.notes;
    ctx.fillText(`💬 ${noteTrunc}`, 28, notesY + 17);
  }

  // ── Bottom bar ──
  const barY = H - 30;
  ctx.fillStyle = '#0f1117';
  ctx.fillRect(0, barY, W, 30);
  ctx.strokeStyle = '#2e3350';
  ctx.beginPath(); ctx.moveTo(0, barY); ctx.lineTo(W, barY); ctx.stroke();

  // Buy URL
  if (item.url) {
    try {
      const domain = new URL(item.url).hostname.replace('www.', '');
      ctx.fillStyle = '#5b8dee';
      ctx.font = '11px monospace';
      ctx.fillText(`🛒 ${domain}`, 20, barY + 20);
    } catch(e) {}
  }

  // App URL right
  ctx.fillStyle = '#4a5070';
  ctx.font = '10px monospace';
  ctx.textAlign = 'right';
  ctx.fillText('art-bot5000.github.io/stockroom', W - 20, barY + 20);
  ctx.textAlign = 'left';
}

function wrapText(ctx, text, maxWidth) {
  const words = text.split(' ');
  const lines = [];
  let current = '';
  for (const word of words) {
    const test = current ? current + ' ' + word : word;
    if (ctx.measureText(test).width > maxWidth && current) {
      lines.push(current);
      current = word;
    } else {
      current = test;
    }
  }
  if (current) lines.push(current);
  return lines;
}

function roundRect(ctx, x, y, w, h, radii) {
  const [tl, tr, br, bl] = Array.isArray(radii) ? radii : [radii, radii, radii, radii];
  ctx.moveTo(x + tl, y);
  ctx.lineTo(x + w - tr, y);
  ctx.quadraticCurveTo(x + w, y, x + w, y + tr);
  ctx.lineTo(x + w, y + h - br);
  ctx.quadraticCurveTo(x + w, y + h, x + w - br, y + h);
  ctx.lineTo(x + bl, y + h);
  ctx.quadraticCurveTo(x, y + h, x, y + h - bl);
  ctx.lineTo(x, y + tl);
  ctx.quadraticCurveTo(x, y, x + tl, y);
}

async function doShareImage() {
  const canvas = document.getElementById('share-canvas');
  if (!canvas || !sharingItem) return;

  canvas.toBlob(async blob => {
    const file = new File([blob], `${sharingItem.name.replace(/\s+/g, '-')}.png`, { type: 'image/png' });
    const shareData = {
      title: sharingItem.name,
      text:  buildShareText(sharingItem),
      files: [file],
    };

    if (navigator.canShare && navigator.canShare({ files: [file] })) {
      try {
        await navigator.share(shareData);
        closeModal('share-modal');
      } catch(e) {
        if (e.name !== 'AbortError') downloadShareImage(canvas, sharingItem);
      }
    } else {
      // Fallback — download the image
      downloadShareImage(canvas, sharingItem);
    }
  }, 'image/png');
}

function downloadShareImage(canvas, item) {
  const a = document.createElement('a');
  a.download = `${item.name.replace(/\s+/g, '-')}.png`;
  a.href = canvas.toDataURL('image/png');
  a.click();
  toast('Image saved — share from your downloads');
}

async function doShareItemLink() {
  if (!sharingItem) return;
  const link = generateItemShareLink(sharingItem);
  const shareData = {
    title: `${sharingItem.name} — via STOCKROOM`,
    text:  `I'm using ${sharingItem.name} in STOCKROOM — here's the item so you can track it too:`,
    url:   link,
  };
  if (navigator.share) {
    try { await navigator.share(shareData); closeModal('share-modal'); return; }
    catch(e) { if (e.name === 'AbortError') return; }
  }
  fallbackCopy(link);
}

async function doShareLink() {
  const url = sharingItem?.url || window.location.href;
  const shareData = { title: sharingItem?.name, text: buildShareText(sharingItem), url };

  if (navigator.share) {
    try { await navigator.share(shareData); closeModal('share-modal'); }
    catch(e) { if (e.name !== 'AbortError') fallbackCopy(url); }
  } else {
    fallbackCopy(url);
  }
}

async function doShareText() {
  if (!sharingItem) return;
  const text = buildShareText(sharingItem);
  fallbackCopy(text);
}

function buildShareText(item) {
  if (!item) return '';
  const history   = getPriceHistory(item);
  const lastPrice = history.length ? (history[history.length-1].raw || `£${history[history.length-1].price.toFixed(2)}`) : null;
  const storePrices = (item.storePrices || []).filter(sp => sp.store && sp.price);

  const lines = [`📦 ${item.name}`];
  if (item.category) lines.push(`Category: ${item.category}`);
  if (lastPrice)     lines.push(`Price: ${lastPrice}`);
  if (item.store)    lines.push(`Available at: ${item.store}`);
  if (storePrices.length > 1) {
    lines.push('Price comparison:');
    storePrices.forEach(sp => lines.push(`  ${sp.store}: ${sp.price}`));
  }
  if (item.qty && item.qty !== 1) lines.push(`Pack size: ×${item.qty}`);
  if (item.rating)   lines.push(`Rated: ${'★'.repeat(item.rating)}${'☆'.repeat(5 - item.rating)}`);
  if (item.notes)    lines.push(`Note: ${item.notes}`);
  if (item.url)      lines.push(`Buy here: ${item.url}`);
  lines.push('');
  lines.push('Shared via STOCKROOM — art-bot5000.github.io/stockroom');
  return lines.join('\n');
}

function fallbackCopy(text) {
  navigator.clipboard?.writeText(text)
    .then(() => { toast('Copied to clipboard ✓'); closeModal('share-modal'); })
    .catch(() => {
      // Manual copy fallback
      const ta = document.createElement('textarea');
      ta.value = text;
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      document.body.removeChild(ta);
      toast('Copied to clipboard ✓');
      closeModal('share-modal');
    });
}


let scannedProductName  = '';
let scannedProductImage = null;

// ═══════════════════════════════════════════
//  ITEM SHARE — SHARE & RECEIVE VIA URL
// ═══════════════════════════════════════════
function generateItemShareLink(item) {
  const payload = {
    v: 1,
    name:        item.name,
    category:    item.category,
    cadence:     item.cadence,
    qty:         item.qty,
    months:      item.months,
    url:         item.url   || '',
    store:       item.store || '',
    notes:       item.notes || '',
    rating:      item.rating || null,
    storePrices: (item.storePrices || []).filter(sp => sp.store && sp.price),
  };
  const encoded = btoa(unescape(encodeURIComponent(JSON.stringify(payload))));
  return `https://art-bot5000.github.io/stockroom/?item=${encoded}`;
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
  try { if (payload.url) domainStr = new URL(payload.url).hostname.replace('www.', ''); } catch(e){}

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
      ${payload.notes ? `<div style="font-size:12px;color:var(--muted);font-style:italic;margin-bottom:14px;padding:8px;background:var(--surface2);border-radius:6px">💬 ${esc(payload.notes)}</div>` : ''}
      ${domainStr ? `<div style="margin-bottom:14px"><a href="${esc(payload.url)}" target="_blank" rel="noopener" style="font-size:12px;color:var(--accent2)">🛒 ${esc(domainStr)} ↗</a></div>` : ''}
      <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px">
        <button class="btn btn-primary" style="flex:1" id="incoming-add-btn">+ Add to my stockroom</button>
        <button class="btn btn-ghost" id="incoming-edit-btn">✏️ Add &amp; set up</button>
        <button class="btn btn-ghost btn-sm" onclick="closeIncomingItem()">Dismiss</button>
      </div>
      <p style="font-size:11px;color:var(--muted);text-align:center">Shared via STOCKROOM · <a href="https://art-bot5000.github.io/stockroom/" style="color:var(--muted)">art-bot5000.github.io/stockroom</a></p>
    </div>`;
  document.body.appendChild(el);
  document.getElementById('incoming-add-btn').onclick  = () => addIncomingItem(payload, false);
  document.getElementById('incoming-edit-btn').onclick = () => addIncomingItem(payload, true);
}

function closeIncomingItem() {
  const el = document.getElementById('incoming-item-overlay');
  if (el) el.remove();
}

async function addIncomingItem(payload, openEdit) {
  closeIncomingItem();
  const newItem = {
    id:          uid(),
    name:        payload.name,
    category:    payload.category  || 'Other',
    cadence:     payload.cadence   || 'monthly',
    qty:         payload.qty       || 1,
    months:      payload.months    || 1,
    url:         payload.url       || '',
    store:       payload.store     || '',
    notes:       payload.notes     || '',
    rating:      payload.rating    || null,
    storePrices: payload.storePrices || [],
    imageUrl:    null,
    logs:        [],
    quickAdded:  false,
    updatedAt:   new Date().toISOString(),
  };
  items.push(newItem);
  await saveData();
  scheduleRender('grid', 'dashboard', 'shopping');
  setTimeout(syncAll, 400);
  if (openEdit) {
    setTimeout(() => { openEditModal(newItem.id); enableItemEdit(); }, 300);
  } else {
    toast(`"${payload.name}" added ✓`);
  }
}

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
  // Pre-fill the search so likely matches show first
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


async function handleShareJoinLink(code) {
  // ECDH share system: no key in URL — key is fetched from server after auth
  updateSyncPill('syncing');
  try {
    // Probe the share code without auth — get metadata
    const probe = await fetchKV(`${WORKER_URL}/share/join`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ code }),
    });
    const probeData = await probe.json();

    if (probe.status === 410) {
      toast('This invite link has expired — ask the owner for a new one');
      updateSyncPill('error');
      return;
    }
    if (!probe.ok && !probeData.requiresAuth) throw new Error(probeData.error || 'Invalid link');

    // Store pending join details for after auth
    _pendingJoinCode  = code.toUpperCase();
    _pendingShareMeta = probeData;

    // If already signed in — join immediately
    if (kvConnected && _kvEmailHash && (_kvVerifier || _kvSessionToken)) {
      await completePendingJoin();
      return;
    }

    // Not signed in — show auth gate
    showShareAuthGate(probeData);
  } catch(err) {
    updateSyncPill('error');
    toast('Invalid invite link — ' + err.message);
  }
}

// Share auth gate — shown when user clicks a link but isn't signed in
function showShareAuthGate(meta) {
  const wizard = document.getElementById('wizard');
  if (!wizard) return;
  wizard.style.display = 'flex';

  // Replace step 1 with share auth gate
  const step1 = document.getElementById('wizard-step-1');
  if (!step1) return;
  const hCount = Object.keys(meta.households || {}).length;
  step1.innerHTML = `
    <div style="font-size:44px;margin-bottom:12px">🏠</div>
    <h1 style="font-size:22px;font-weight:700;margin-bottom:6px">You're invited!</h1>
    <p style="color:var(--muted);font-size:13px;line-height:1.6;margin-bottom:16px">
      <strong style="color:var(--text)">${esc(meta.ownerName||'Someone')}</strong> has invited you
      to access ${hCount} household${hCount!==1?'s':''} as a <strong>${esc(meta.type||'guest')}</strong>.
    </p>
    <div style="background:var(--surface2);border:1px solid var(--border);border-radius:10px;padding:12px;margin-bottom:20px;text-align:left">
      ${Object.entries(meta.households||{}).map(([hKey,perms]) => {
        const hName = meta.householdNames?.[hKey] || (hKey==='default'?'Home':hKey);
        const sections = Object.entries(perms).filter(([,v])=>v!=='none');
        return `<div style="margin-bottom:6px">
          <div style="font-size:12px;font-weight:700;margin-bottom:3px">🏠 ${esc(hName)}</div>
          <div style="display:flex;gap:4px;flex-wrap:wrap">
            ${sections.map(([s,v])=>`<span style="font-size:10px;padding:1px 6px;border-radius:99px;
              background:${v==='rw'?'rgba(76,187,138,0.15)':'rgba(91,141,238,0.15)'};
              color:${v==='rw'?'var(--ok)':'var(--accent)'}">
              ${SECTION_LABELS[s]||s} ${v==='rw'?'✏️':'👁'}</span>`).join('')}
          </div>
        </div>`;
      }).join('')}
    </div>
    <p style="font-size:13px;font-weight:600;margin-bottom:12px">Sign in or create an account to accept:</p>
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
  const email      = document.getElementById('share-gate-email')?.value.trim();
  const passphrase = document.getElementById('share-gate-pass')?.value;
  const errEl      = document.getElementById('share-gate-error');
  if (!email || !passphrase) { if(errEl){errEl.textContent='Enter email and passphrase';errEl.style.display='block';} return; }
  try {
    const emailHash = await kvHashEmail(email);
    const verifier  = await kvMakeVerifier(passphrase, emailHash);
    const res = await fetchKV(`${WORKER_URL}/user/verify`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash, verifier }),
    });
    const d = await res.json();
    if (res.status === 404) throw new Error('Account not found — use Create new account');
    if (!res.ok) throw new Error(d.error || 'Sign-in failed');
    const key = await kvDeriveKey(email, passphrase);
    await kvStoreSession(email, emailHash, verifier, key);
    await offerTrustDevice(email, emailHash, verifier, key);
    await completePendingJoin();
  } catch(err) {
    const errEl = document.getElementById('share-gate-error');
    if (errEl) { errEl.textContent = err.message; errEl.style.display = 'block'; }
  }
}

async function shareGateRegister() {
  const email      = document.getElementById('share-gate-email')?.value.trim();
  const passphrase = document.getElementById('share-gate-pass')?.value;
  const errEl      = document.getElementById('share-gate-error');
  if (!email || !passphrase) { if(errEl){errEl.textContent='Enter email and passphrase';errEl.style.display='block';} return; }
  if (passphrase.length < 8) { if(errEl){errEl.textContent='Passphrase must be at least 8 characters';errEl.style.display='block';} return; }
  try {
    const emailHash = await kvHashEmail(email);
    const verifier  = await kvMakeVerifier(passphrase, emailHash);
    // Always use v2 for new registrations.
    const useV2     = true;
    const res = await fetchKV(`${WORKER_URL}/user/register`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash, verifier, email }),
    });
    const d = await res.json();
    if (!res.ok) throw new Error(d.error || 'Registration failed');

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

    // Verify email before completing join
    await showEmailVerification(email, emailHash, async () => {
      await offerTrustDevice(email, emailHash, verifier, dataKey);
      await completePendingJoin();
    });
  } catch(err) {
    const e2 = document.getElementById('share-gate-error');
    if (e2) { e2.textContent = err.message; e2.style.display = 'block'; }
  }
}

// Complete the pending join after authentication — ECDH key unwrap
async function completePendingJoin() {
  if (!_pendingJoinCode) return;
  const code = _pendingJoinCode;
  try {
    // 1. Authenticate join on server
    const res = await fetchKV(`${WORKER_URL}/share/join`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        code,
        guestEmailHash: _kvEmailHash,
        ...(_kvSessionToken ? { guestSessionToken: _kvSessionToken } : { guestVerifier: _kvVerifier }),
      }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Could not join');

    // 2. Fetch ECDH-wrapped share key from server
    const ecdhRes = await fetchKV(`${WORKER_URL}/share/ecdh-key/get`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        guestEmailHash: _kvEmailHash,
        ...(_kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier }),
        code,
      }),
    });
    if (!ecdhRes.ok) throw new Error('Could not retrieve your share key — ask the owner to re-send the invite');
    const { wrappedKey, ownerPublicKeyJwk } = await ecdhRes.json();

    // 3. Load guest private key and unwrap share key
    const guestPrivKey = await loadEcdhPrivateKey(_kvEmailHash);
    if (!guestPrivKey) throw new Error('Your encryption key is missing — sign out and back in to regenerate it');
    const shareKey = await ecdhUnwrapShareKey(guestPrivKey, ownerPublicKeyJwk, wrappedKey);

    // 4. Export and cache share key locally
    const shareKeyB64 = await exportShareKey(shareKey);
    try {
      const stored = JSON.parse(localStorage.getItem('stockroom_share_keys') || '{}');
      stored[code] = shareKeyB64;
      localStorage.setItem('stockroom_share_keys', JSON.stringify(stored));
    } catch(e) {}

    // 5. Set share state
    _shareState = { ...data, code };
    _shareKey   = shareKey;
    saveShareState();

    _pendingJoinCode  = null;
    _pendingShareMeta = null;

    // 6. Enter app
    localStorage.setItem('stockroom_seen', '1');
    localStorage.setItem('stockroom_country_set', '1');
    document.body.classList.remove('wizard-active');
    document.getElementById('wizard').style.display = 'none';
    applyTabPermissions();
    updateSyncPill('syncing');
    await kvSyncNow();
    scheduleRender(...RENDER_REGIONS);
    history.replaceState(null, '', location.pathname + location.search);
    toast(`Joined ${data.ownerName||'household'}'s STOCKROOM ✓`);
  } catch(err) {
    toast('Could not join: ' + err.message);
    updateSyncPill('error');
  }
}

// Legacy — replaced by showShareAuthGate
function showShareWizard(shareData) { showShareAuthGate(shareData); }
async function acceptShareAndContinue() { await completePendingJoin(); }

function showShareJoinConfirm(shareData) {
  // Brief modal confirming what they have access to
  const html = `
    <div style="text-align:center;padding:8px 0 16px">
      <div style="font-size:40px;margin-bottom:12px">✓</div>
      <h3 style="margin-bottom:8px">Joined ${esc(shareData.ownerName || 'household')}'s STOCKROOM</h3>
      <p style="font-size:13px;color:var(--muted);margin-bottom:16px">
        You have access as a <strong>${esc(shareData.type || 'guest')}</strong>.
        
      </p>
      <button class="btn btn-primary full" onclick="closeModal('share-confirm-modal')">Got it</button>
    </div>`;
  // Use a simple toast since we don't want another modal
  toast(`✓ Joined ${shareData.ownerName || 'household'}'s STOCKROOM as ${shareData.type || 'guest'}`);
}

function handleURLAction() {
  // ── Share join link: ?join=CODE ───────────────────────────
  const joinParams = new URLSearchParams(location.search);
  const joinCode   = joinParams.get('join');
  if (joinCode) {
    history.replaceState(null, '', location.pathname);
    handleShareJoinLink(joinCode.toUpperCase());
    return;
  }

  // Check for incoming shared item first
  if (checkIncomingItem()) return;

  const params = new URLSearchParams(location.search);
  const action = params.get('action');
  if (!action) return;

  // Clean the URL so refreshing doesn't re-trigger
  history.replaceState(null, '', location.pathname);

  // Wait until the app is fully rendered before opening modals
  setTimeout(() => {
    if (action === 'quick-add') {
      openQuickAdd();
    } else if (action === 'log-purchase') {
      openLogPicker();
    } else if (action === 'shopping') {
      showShoppingListInline();
    } else if (action === 'scan') {
      sessionStorage.setItem('barcode_target', 'scan-chooser');
      openBarcodeScanner();
    } else if (action === 'reminder-sync') {
      // Fired when user taps "Open STOCKROOM" from the email confirmation page
      const id    = params.get('id')    || '';
      const date  = params.get('date')  || today();
      const token = params.get('token') || '';
      if (id && token) applyReminderReplaced(id, date, token);
    } else if (action === 'unsubscribe') {
      handleUnsubscribe();
    } else if (action === 'share') {
      // Share target — user shared a URL/title from Amazon, Tesco, etc.
      const title = params.get('title') || '';
      const text  = params.get('text')  || '';
      const url   = params.get('url')   || '';

      // If we have a URL, open the full Add Item modal pre-filled
      if (url) {
        const detectedStore = urlToStoreName(url);
        const productName   = title || text || '';

        // Switch to Stockroom tab first
        const stockTab = [...document.querySelectorAll('.tab')].find(t => t.textContent.includes('Stockroom'));
        if (stockTab) showView('stock', stockTab);

        openAddModal();
        setTimeout(() => {
          // Pre-fill URL and auto-detect store
          document.getElementById('f-url').value = url;
          autoFillStore();
          // Pre-fill name if we have one from title/text
          if (productName) {
            document.getElementById('f-name').value = productName;
            updatePriceLinks();
          }
          // Focus name field — user just needs to confirm/edit the name
          const nameField = document.getElementById('f-name');
          if (nameField) nameField.focus();
          // Show a subtle hint
          toast(`🛒 ${detectedStore ? detectedStore + ' link' : 'Link'} added — fill in the details`);
        }, 200);
      } else {
        // No URL — fall back to Quick Add with the text
        const name = title || text || '';
        openQuickAdd();
        if (name) {
          const ta = document.getElementById('quick-add-input');
          if (ta) { ta.value = name; updateQuickAddPreview(); }
        }
      }
    }
  }, 600);
}

// ── Log Purchase Picker ───────────────────
function openLogPicker() {
  renderLogPickerList('');
  document.getElementById('log-picker-search').value = '';
  openModal('log-picker-modal');
  setTimeout(() => document.getElementById('log-picker-search').focus(), 100);
}

function renderLogPickerList(filter) {
  const list = document.getElementById('log-picker-list');
  if (!list) return;

  const q = filter.toLowerCase().trim();
  const sorted = [...items]
    .filter(i => !i.quickAdded)
    .filter(i => !q || i.name.toLowerCase().includes(q))
    .sort((a, b) => {
      // Most recently bought first
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

function filterLogPicker(val) {
  renderLogPickerList(val);
}

function pickItemForLog(id) {
  closeModal('log-picker-modal');
  openLogModal(id);
}


function openQuickAdd() {
  document.getElementById('quick-add-input').value = '';
  document.getElementById('quick-add-preview').style.display = 'none';
  document.getElementById('quick-add-chips').innerHTML = '';
  openModal('quick-add-modal');
  setTimeout(() => document.getElementById('quick-add-input').focus(), 100);
}

function parseQuickAddNames() {
  const raw = document.getElementById('quick-add-input').value;
  return raw.split(',')
    .map(s => s.trim())
    .filter(s => s.length > 0);
}

function updateQuickAddPreview() {
  const names   = parseQuickAddNames();
  const preview = document.getElementById('quick-add-preview');
  const chips   = document.getElementById('quick-add-chips');
  const btn     = document.getElementById('quick-add-save-btn');
  if (!names.length) {
    preview.style.display = 'none';
    return;
  }
  preview.style.display = 'block';
  chips.innerHTML = names.map(n =>
    `<span style="display:inline-flex;align-items:center;gap:5px;padding:4px 10px;background:var(--surface2);border:1px solid var(--border);border-radius:99px;font-size:12px;color:var(--text)">
      📦 ${esc(n)}
    </span>`
  ).join('');
  btn.textContent = `⚡ Add ${names.length} Item${names.length !== 1 ? 's' : ''}`;
}

async function saveQuickAdd() {
  const names = parseQuickAddNames();
  if (!names.length) { toast('Enter at least one item name'); return; }

  const now = new Date().toISOString();
  names.forEach(name => {
    items.push({
      id:          uid(),
      name,
      category:    'Other',
      cadence:     'monthly',
      qty:         1,
      months:      1,
      url:         '',
      store:       '',
      notes:       '',
      rating:      null,
      imageUrl:    null,
      logs:        [],
      storePrices: [],
      quickAdded:  true,   // ← flag for incomplete tracking
      updatedAt:   now,
    });
  });

  await saveData();
  closeModal('quick-add-modal');
  scheduleRender('grid', 'dashboard', 'filters', 'shopping');
  setTimeout(syncAll, 400);
  toast(`${names.length} item${names.length !== 1 ? 's' : ''} added — complete their details when ready`);
}

// Called from barcode scanner in quick-add context
function quickAddBarcodeScan() {
  // Temporarily redirect barcode result to the quick-add textarea
  sessionStorage.setItem('barcode_target', 'quick-add');
  openBarcodeScanner();
}
