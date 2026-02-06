// static/js/markdown.js
// Handles loading Markdown from the backend and inserting it into
// the appropriate DOM containers (Intro, Experiment Design, Tech Details,
// and the two Directions modals). Also re-runs MathJax so inline
// equations like $\frac{T_{experiment}}{T_{reference}}$ render correctly.

import { debugLog } from "./plots.js";

let markdownRenderer = null;

function getMarkdownRenderer() {
  if (!markdownRenderer) {
    if (window.markdownit) {
      markdownRenderer = window.markdownit({
        html: true,    // allow embedded HTML (figures, tables, etc.)
        linkify: true,
        breaks: false,
      });
    } else {
      console.error("❌ markdown-it is not available on window.markdownit");
    }
  }
  return markdownRenderer;
}

/**
 * Fetch a Markdown file, render it to HTML, and inject into the container.
 * Then, if MathJax is present, re-typeset that container so TeX equations render.
 */
// markdown.js
import { showLoadingOverlay, hideLoadingOverlay } from "./ui_loading.js";

export async function loadMarkdownContent(containerId, markdownPath) {
  const container = document.getElementById(containerId);
  if (!container) {
    console.error(`❌ Markdown container #${containerId} not found`);
    return;
  }

  debugLog(`📖 Loading markdown for #${containerId} from ${markdownPath} …`);

  // Optional: if you have a <div id="intro-status"> etc.
  const statusEl = document.getElementById(`${containerId}-status`);

  // Create a human-friendly label from id: "intro-content" -> "introduction"
  const pretty =
    containerId === "intro-content"
      ? "introduction"
      : containerId
          .replace(/-content$/i, "")
          .replace(/[-_]/g, " ")
          .trim();

  try {
    // ✅ Animated overlay (dots)
    showLoadingOverlay(container, `Loading ${pretty}`);

    if (statusEl) {
      statusEl.textContent = `Loading ${pretty}…`;
      statusEl.style.display = "";
    }

    const resp = await fetch(markdownPath, { cache: "no-store" });
    if (!resp.ok) {
      const msg = `Failed to fetch ${markdownPath}: HTTP ${resp.status}`;
      console.error(`❌ ${msg}`);

      container.innerHTML = `<div class="alert alert-warning mb-0">${msg}</div>`;
      return;
    }

    const text = await resp.text();
    const renderer = getMarkdownRenderer();

    const html = renderer ? renderer.render(text) : text;

    // Apply our markdown styling class and inject content
    container.classList.add("markdown-content");
    container.innerHTML = html;

    // 🔢 Re-run MathJax on this container so inline/display math renders
    if (window.MathJax && window.MathJax.typesetPromise) {
      window.MathJax.typesetPromise([container]).catch((err) => {
        console.error("❌ MathJax typeset error:", err);
      });
    }

    debugLog(`✅ Markdown loaded into #${containerId}`);
  } catch (err) {
    console.error(`❌ Error loading markdown for #${containerId}:`, err);
    container.innerHTML =
      `<div class="alert alert-danger mb-0">` +
      `Error loading content. Please refresh the page.</div>`;
  } finally {
    // ✅ Always remove overlay + status
    hideLoadingOverlay(container);
    if (statusEl) statusEl.style.display = "none";
  }
}