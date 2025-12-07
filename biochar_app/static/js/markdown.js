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
export async function loadMarkdownContent(containerId, markdownPath) {
  const container = document.getElementById(containerId);
  if (!container) {
    console.error(`❌ Markdown container #${containerId} not found`);
    return;
  }

  debugLog(`📖 Loading markdown for #${containerId} from ${markdownPath} …`);

  try {
    const resp = await fetch(markdownPath);
    if (!resp.ok) {
      console.error(
        `❌ Failed to fetch ${markdownPath}: HTTP ${resp.status}`
      );
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
  }
}