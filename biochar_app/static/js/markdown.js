// @ts-check
// static/js/markdown.js
// Handles loading Markdown from the backend and inserting it into
// the appropriate DOM containers (Intro, Experiment Design, Tech Details,
// and the two Directions modals). Also re-runs MathJax so inline
// equations like $\frac{T_{experiment}}{T_{reference}}$ render correctly.

import { debugLog } from "./debug_utils.js";
import { showLoadingOverlay, hideLoadingOverlay } from "./ui_loading.js";

/**
 * @typedef {{
 *   render: (text: string) => string
 * }} MarkdownRenderer
 */

/**
 * @typedef {Window & {
 *   markdownit?: (options?: Record<string, any>) => MarkdownRenderer,
 *   MathJax?: {
 *     typesetPromise?: (elements?: Element[]) => Promise<void>
 *   }
 * }} MarkdownWindow
 */

/** @type {MarkdownWindow} */
const markdownWindow = /** @type {MarkdownWindow} */ (window);

/** @type {MarkdownRenderer | null} */
let markdownRenderer = null;

/**
 * @returns {MarkdownRenderer | null}
 */
function getMarkdownRenderer() {
  if (!markdownRenderer) {
    if (markdownWindow.markdownit) {
      markdownRenderer = markdownWindow.markdownit({
        html: true,
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
 * @param {string} containerId
 * @returns {string}
 */
function prettyNameFromContainerId(containerId) {
  if (containerId === "intro-content") return "introduction";
  if (containerId === "experiment-content") return "experiment design";
  if (containerId === "tech-content") return "technical details";
  if (containerId === "acknowledgements-content") return "acknowledgement details";
  if (containerId.startsWith("modal-")) return "help";
  return containerId.replace(/-content$/i, "").replace(/[-_]/g, " ").trim();
}

/**
 * Fetch a Markdown file, render it to HTML, and inject into the container.
 * Then, if MathJax is present, re-typeset that container so TeX equations render.
 *
 * @param {string} containerId
 * @param {string} markdownPath
 * @returns {Promise<void>}
 */
export async function loadMarkdownContent(containerId, markdownPath) {
  const container = document.getElementById(containerId);
  if (!container) {
    console.error(`❌ Markdown container #${containerId} not found`);
    return;
  }

  debugLog(`📖 Loading markdown for #${containerId} from ${markdownPath} …`);

  const baseId = containerId.replace(/-content$/i, "");
  const statusEl = document.getElementById(`${baseId}-status`);

  const pretty = prettyNameFromContainerId(containerId);

  try {
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

    container.classList.add("markdown-content");
    container.innerHTML = html;

    if (markdownWindow.MathJax && markdownWindow.MathJax.typesetPromise) {
      markdownWindow.MathJax.typesetPromise([container]).catch(
        /**
         * @param {unknown} err
         */
        (err) => {
          console.error("❌ MathJax typeset error:", err);
        }
      );
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