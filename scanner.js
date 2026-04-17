// ═══════════════════════════════════════════
//  BARCODE SCANNER
//  All other functions previously in this file
//  have been moved to app.js where they belong.
// ═══════════════════════════════════════════
let barcodeStream   = null;
let barcodeInterval = null;

async function openBarcodeScanner() {
  if (!('BarcodeDetector' in window)) {
    toast('Barcode scanning not supported on this browser — try Chrome on Android');
    return;
  }
  openModal('barcode-modal');
  const video    = document.getElementById('barcode-video');
  const statusEl = document.getElementById('barcode-status');
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
  const statusEl      = document.getElementById('barcode-status');
  const isQuickAdd    = sessionStorage.getItem('barcode_target') === 'quick-add';
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
        openScanChooser(productName, imageUrl);
      } else if (isQuickAdd) {
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

// Called from barcode scanner in quick-add context
function quickAddBarcodeScan() {
  sessionStorage.setItem('barcode_target', 'quick-add');
  openBarcodeScanner();
}
