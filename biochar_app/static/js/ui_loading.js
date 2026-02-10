// static/js/ui_loading.js

function ensurePositioned(el) {
  const pos = window.getComputedStyle(el).position;
  if (pos === "static") {
    el.dataset._prevPosition = "static";
    el.style.position = "relative";
  }
}

function ensureMinHeight(el, px = 160) {
  const minH = window.getComputedStyle(el).minHeight;
  const numeric = parseFloat(minH || "0") || 0;
  if (numeric < 1) {
    el.dataset._prevMinHeight = el.style.minHeight || "";
    el.style.minHeight = `${px}px`;
  }
}

/* =========================================================
   In-container overlay (plots, tables, markdown containers)
   ========================================================= */

export function showLoadingOverlay(container, label = "Loading") {
  if (!container) return;

  // If already exists, don’t stack multiples
  const existing = container.querySelector(":scope > .loading-overlay");
  if (existing) return;

  ensurePositioned(container);
  ensureMinHeight(container, 160);

  const overlay = document.createElement("div");
  overlay.className = "loading-overlay";
  overlay.setAttribute("role", "status");
  overlay.setAttribute("aria-live", "polite");

  overlay.innerHTML = `
    <div class="loading-overlay-inner">
      <span>${label}</span><span class="dots"></span>
    </div>
  `;

  container.appendChild(overlay);
}

export function hideLoadingOverlay(container) {
  if (!container) return;

  const overlay = container.querySelector(":scope > .loading-overlay");
  if (overlay) overlay.remove();

  // Restore temporary styling if we changed it
  if (container.dataset._prevPosition === "static") {
    container.style.position = "";
    delete container.dataset._prevPosition;
  }

  if ("_prevMinHeight" in container.dataset) {
    container.style.minHeight = container.dataset._prevMinHeight;
    delete container.dataset._prevMinHeight;
  }
}

/* =========================================================
   Status-line loading dots (e.g., summary-status)
   ========================================================= */

export function startLoadingDots(elId, baseText = "Loading") {
  const el = document.getElementById(elId);
  if (!el) return null;

  stopLoadingDots("");

  let dots = 0;
  el.textContent = baseText;

  const timer = window.setInterval(() => {
    dots = (dots + 1) % 4;
    el.textContent = baseText + ".".repeat(dots);
  }, 350);

  el.dataset.loadingTimer = String(timer);
  el.style.display = "";
  return timer;
}

export function stopLoadingDots(elId, finalText = "") {
  const el = document.getElementById(elId);
  if (!el) return;

  const raw = el.dataset.loadingTimer;
  if (raw) {
    window.clearInterval(parseInt(raw, 10));
    delete el.dataset.loadingTimer;
  }

  // Always clear or set text
  el.textContent = finalText;
}

/* =========================================================
   Boot-level page loader (covers whole screen on first load)
   ========================================================= */

export function hideBootLoading() {
  const el = document.getElementById("boot-loading");
  if (!el) return;

  // Remove it entirely so it can't block clicks ever again
  el.remove();
}

export function showBootLoading(label = "Loading site") {
  // Optional helper if you ever want to re-enable
  let el = document.getElementById("boot-loading");
  if (!el) {
    el = document.createElement("div");
    el.id = "boot-loading";
    el.className = "boot-loading";
    el.innerHTML = `<div class="boot-loading-inner"><span></span><span class="dots"></span></div>`;
    document.body.appendChild(el);
  }

  const textSpan = el.querySelector(".boot-loading-inner > span");
  if (textSpan) textSpan.textContent = label;

  el.style.display = "flex";
}