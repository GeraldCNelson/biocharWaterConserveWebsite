// static/js/ui_loading.js

function ensurePositioned(el) {
  const pos = window.getComputedStyle(el).position;
  if (pos === "static") {
    // Mark that we changed it so we can restore later
    el.dataset._prevPosition = "static";
    el.style.position = "relative";
  }
}

function ensureMinHeight(el, px = 160) {
  const computed = window.getComputedStyle(el).minHeight;
  const numeric = parseFloat(computed || "0");
  const minH = Number.isFinite(numeric) ? numeric : 0;

  // If min-height is unset/0/auto-ish, set a temporary one for loading visibility.
  if (minH < 1) {
    el.dataset._prevMinHeight = el.style.minHeight || "";
    el.style.minHeight = `${px}px`;
  }
}

function buildOverlayHTML(label) {
  // Keep it simple; label comes from our code (not user input)
  return `
    <div class="loading-overlay__inner">
      <span class="loading-overlay__label">${label}</span><span class="dots"></span>
    </div>
  `;
}

export function showLoadingOverlay(container, label = "Loading") {
  if (!container) return;

  // Find any existing overlay (don’t depend on :scope)
  let overlay = container.querySelector(".loading-overlay");

  // If exists, just update the label + return (prevents duplicates, keeps message current)
  if (overlay) {
    const labelEl = overlay.querySelector(".loading-overlay__label");
    if (labelEl) labelEl.textContent = label;
    return;
  }

  ensurePositioned(container);
  ensureMinHeight(container, 160);

  overlay = document.createElement("div");
  overlay.className = "loading-overlay";
  overlay.setAttribute("role", "status");
  overlay.setAttribute("aria-live", "polite");

  overlay.innerHTML = buildOverlayHTML(label);

  container.appendChild(overlay);
}

export function hideLoadingOverlay(container) {
  if (!container) return;

  const overlay = container.querySelector(".loading-overlay");
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