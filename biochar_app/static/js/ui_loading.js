function ensureRelative(el) {
  const style = window.getComputedStyle(el);
  if (style.position === "static") el.style.position = "relative";
}

export function showLoadingOverlay(containerEl, message = "Loading") {
  if (!containerEl) return;
  ensureRelative(containerEl);

  let overlay = containerEl.querySelector(":scope > .loading-overlay");
  if (!overlay) {
    overlay = document.createElement("div");
    overlay.className = "loading-overlay";
    overlay.innerHTML = `<span class="msg">${message}<span class="dots"></span></span>`;
    containerEl.appendChild(overlay);
  } else {
    overlay.querySelector(".msg").firstChild.nodeValue = message;
    overlay.style.display = "flex";
  }
}

export function hideLoadingOverlay(containerEl) {
  const overlay = containerEl?.querySelector?.(":scope > .loading-overlay");
  if (overlay) overlay.style.display = "none";
}