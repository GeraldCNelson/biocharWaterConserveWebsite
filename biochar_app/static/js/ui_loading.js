// @ts-check
// ui_loading.js

/**
 * Ensure the container has non-static positioning so an absolute overlay
 * can be placed inside it safely.
 *
 * @param {HTMLElement | null} el
 * @returns {void}
 */
function ensurePositioned(el) {
  if (!el) return;

  const pos = window.getComputedStyle(el).position;
  if (pos === "static") {
    el.dataset._prevPosition = "static";
    el.style.position = "relative";
  }
}

/**
 * Ensure the container has a minimum height while loading so the overlay
 * has visible space even before Plotly finishes rendering.
 *
 * @param {HTMLElement | null} el
 * @param {number} [px=160]
 * @returns {void}
 */
function ensureMinHeight(el, px = 160) {
  if (!el) return;

  const minH = window.getComputedStyle(el).minHeight;
  const minHNum = parseFloat(minH);

  if (!Number.isFinite(minHNum) || minHNum < px) {
    el.dataset._prevMinHeight = el.style.minHeight || "";
    el.style.minHeight = `${px}px`;
  }
}

/**
 * Show a loading overlay inside the given container.
 *
 * @param {HTMLElement | null} container
 * @param {string} [label="Loading"]
 * @returns {void}
 */
export function showLoadingOverlay(container, label = "Loading") {
  if (!container) return;

  ensurePositioned(container);
  ensureMinHeight(container);

  const existing = container.querySelector(".plot-loading-overlay");
  if (existing) return;

  const overlay = document.createElement("div");
  overlay.className = "plot-loading-overlay";
  overlay.innerHTML = `
    <div class="plot-loading-overlay__inner">
      <div class="plot-loading-overlay__spinner"></div>
      <div class="plot-loading-overlay__label">${label}…</div>
    </div>
  `;

  Object.assign(overlay.style, {
    position: "absolute",
    inset: "0",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    background: "rgba(255,255,255,0.72)",
    zIndex: "20",
    pointerEvents: "none",
  });

  const inner = /** @type {HTMLElement | null} */ (overlay.firstElementChild);
  if (inner) {
    Object.assign(inner.style, {
      display: "flex",
      flexDirection: "column",
      alignItems: "center",
      gap: "10px",
      fontSize: "14px",
      color: "#333",
    });
  }

  const spinner = /** @type {HTMLElement | null} */ (
    overlay.querySelector(".plot-loading-overlay__spinner")
  );
  if (spinner) {
    Object.assign(spinner.style, {
      width: "28px",
      height: "28px",
      border: "3px solid rgba(0,0,0,0.15)",
      borderTop: "3px solid rgba(0,0,0,0.55)",
      borderRadius: "50%",
      animation: "plot-loading-spin 0.8s linear infinite",
    });
  }

  container.appendChild(overlay);

  if (!document.getElementById("plot-loading-overlay-style")) {
    const style = document.createElement("style");
    style.id = "plot-loading-overlay-style";
    style.textContent = `
      @keyframes plot-loading-spin {
        from { transform: rotate(0deg); }
        to { transform: rotate(360deg); }
      }
    `;
    document.head.appendChild(style);
  }
}

/**
 * Hide the loading overlay inside the given container and restore any
 * temporary sizing/positioning adjustments.
 *
 * @param {HTMLElement | null} container
 * @returns {void}
 */
export function hideLoadingOverlay(container) {
  if (!container) return;

  const overlay = container.querySelector(".plot-loading-overlay");
  if (overlay) {
    overlay.remove();
  }

  if (container.dataset._prevPosition === "static") {
    container.style.position = "static";
    delete container.dataset._prevPosition;
  }

  if ("_prevMinHeight" in container.dataset) {
    container.style.minHeight = container.dataset._prevMinHeight || "";
    delete container.dataset._prevMinHeight;
  }
}

/** @type {Record<string, number>} */
const loadingDotTimers = {};

/**
 * Start animated loading dots in a text element.
 *
 * @param {string} elId
 * @param {string} [baseText="Loading"]
 * @returns {void}
 */
export function startLoadingDots(elId, baseText = "Loading") {
  const el = document.getElementById(elId);
  if (!el) return;

  stopLoadingDots(elId);

  let count = 0;
  el.textContent = baseText;

  loadingDotTimers[elId] = window.setInterval(() => {
    count = (count + 1) % 4;
    el.textContent = `${baseText}${".".repeat(count)}`;
  }, 400);
}

/**
 * Stop animated loading dots and optionally set final text.
 *
 * @param {string} elId
 * @param {string} [finalText=""]
 * @returns {void}
 */
export function stopLoadingDots(elId, finalText = "") {
  const timerId = loadingDotTimers[elId];
  if (timerId) {
    window.clearInterval(timerId);
    delete loadingDotTimers[elId];
  }

  const el = document.getElementById(elId);
  if (!el) return;

  el.textContent = finalText;
}