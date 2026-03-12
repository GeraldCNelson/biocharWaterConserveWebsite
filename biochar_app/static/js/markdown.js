// static/js/markdown.js
// Handles loading Markdown from the backend and inserting it into
// the appropriate DOM containers (Intro, Experiment Design, Tech Details,
// and the two Directions modals). Also re-runs MathJax so inline
// equations like $\frac{T_{experiment}}{T_{reference}}$ render correctly.

import { debugLog } from "./debug_utils.js";
import { showLoadingOverlay, hideLoadingOverlay } from "./ui_loading.js";

let markdownRenderer = null;

function getMarkdownRenderer() {
  if (!markdownRenderer) {
    if (window.markdownit) {
      markdownRenderer = window.markdownit({
        html: true, // allow embedded HTML (figures, tables, etc.)
        linkify: true,
        breaks: false,
      });
    } else {
      console.error("❌ markdown-it is not available on window.markdownit");
    }
  }
  return markdownRenderer;
}

function prettyNameFromContainerId(containerId) {
  if (containerId === "intro-content") return "introduction";
  if (containerId === "experiment-content") return "experiment design";
  if (containerId === "tech-content") return "technical details";
  if (containerId.startsWith("modal-")) return "help";
  return containerId.replace(/-content$/i, "").replace(/[-_]/g, " ").trim();
}

/**
 * Fetch a Markdown file, render it to HTML, and inject into the container.
 * Then, if MathJax is present, re-typeset that container so TeX equations render.
 */
export async function loadMarkdownContent(containerId, markdownPath) {
  const container = document.getElementById(containerId);
  if (!container) {
    console.error(`❌ Markdown container #${containerId} not found`);
    return;
  }

  debugLog(`📖 Loading markdown for #${containerId} from ${markdownPath} …`);

  // ✅ Status line convention:
  // For "intro-content" we look for "intro-status" (NOT "intro-content-status")
  const baseId = containerId.replace(/-content$/i, "");
  const statusEl = document.getElementById(`${baseId}-status`);

  const pretty = prettyNameFromContainerId(containerId);

  try {
    // ✅ Animated overlay (dots) on the container itself
    showLoadingOverlay(container, `Loading ${pretty}`);

    if (statusEl) {
      statusEl.textContent = `Loading ${pretty}…`;
      statusEl.style.display = "";
    }

    // fetch markdown/html produced by backend
    const resp = await fetch(markdownPath, { cache: "no-store" });
    if (!resp.ok) {
      const msg = `Failed to fetch ${markdownPath}: HTTP ${resp.status}`;
      console.error(`❌ ${msg}`);
      container.innerHTML = `<div class="alert alert-warning mb-0">${msg}</div>`;
      return;
    }

    const text = await resp.text();
    const renderer = getMarkdownRenderer();

    // markdown-it will render markdown; if you are serving raw HTML, it's still safe
    // because html:true allows it through.
    const html = renderer ? renderer.render(text) : text;

    container.classList.add("markdown-content");
    container.innerHTML = html;

    // Re-run MathJax in this container
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
    hideLoadingOverlay(container);
    if (statusEl) statusEl.style.display = "none";
  }
}