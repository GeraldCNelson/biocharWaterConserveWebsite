// @ts-check

/**
 * Stable, public names for the website's major tabs. Aliases let older or
 * more descriptive links keep working without coupling report URLs to the
 * Bootstrap button IDs used by the page template.
 */
export const DEEP_LINK_TABS = Object.freeze({
  intro: { buttonId: "intro-tab", paneId: "intro", aliases: ["introduction"] },
  experiment: {
    buttonId: "experiment-tab",
    paneId: "experiment",
    aliases: ["experiment-design"],
  },
  monitoring: {
    buttonId: "main-tab",
    paneId: "main",
    aliases: ["main", "interactive-plots"],
  },
  summary: {
    buttonId: "summary-tab",
    paneId: "summary",
    aliases: ["summary-statistics"],
  },
  seasons: {
    buttonId: "gseason-tab",
    paneId: "gseason",
    aliases: ["gseason", "seasonal-comparison"],
  },
  "biochar-analysis": {
    buttonId: "biochar-analysis-tab",
    paneId: "biochar-analysis",
    aliases: ["biochar-characteristics"],
  },
  "pasture-quality": {
    buttonId: "nir-tab",
    paneId: "nir",
    aliases: ["nir"],
  },
  biomass: {
    buttonId: "biomass-field-tab",
    paneId: "biomass-field",
    aliases: ["biomass-field", "fertilizer"],
  },
  soilbio: {
    buttonId: "soilbio-tab",
    paneId: "soilbio",
    aliases: ["soil-biology", "soil-biological-health"],
  },
  soilchem: {
    buttonId: "soilchem-tab",
    paneId: "soilchem",
    aliases: ["soil-chemistry"],
  },
  "technical-details": {
    buttonId: "tech-details-tab",
    paneId: "tech-details",
    aliases: ["tech-details"],
  },
  downloads: {
    buttonId: "bulk-tab",
    paneId: "bulk-pane",
    aliases: ["bulk", "bulk-downloads"],
  },
  glossary: { buttonId: "glossary-tab", paneId: "glossary", aliases: [] },
  acknowledgements: {
    buttonId: "acknowledgements-tab",
    paneId: "acknowledgements",
    aliases: ["acknowledgments"],
  },
});

const SUMMARY_PARAMETERS = ["year", "granularity", "variable", "strip", "depth"];

/**
 * Convert visible heading text into the stable lowercase form used in URLs.
 *
 * @param {string} value
 * @returns {string}
 */
export function slugifyDeepLinkText(value) {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .replace(/-{2,}/g, "-");
}

/**
 * @param {string} paneId
 * @param {string} headingText
 * @returns {string}
 */
export function makeDeepLinkAnchorId(paneId, headingText) {
  const paneSlug = slugifyDeepLinkText(paneId);
  const headingSlug = slugifyDeepLinkText(headingText);
  return headingSlug ? `${paneSlug}-${headingSlug}` : paneSlug;
}

/**
 * @param {string | null} requested
 * @returns {{slug: string, buttonId: string, paneId: string, aliases: readonly string[]} | null}
 */
function resolveTab(requested) {
  if (!requested) return null;
  const normalized = slugifyDeepLinkText(requested);

  for (const [slug, config] of Object.entries(DEEP_LINK_TABS)) {
    if (slug === normalized || config.aliases.includes(normalized)) {
      return { slug, ...config };
    }
  }
  return null;
}

/**
 * Add IDs to headings rendered from Markdown or JSON after the initial page
 * load. Existing authored IDs are retained. Duplicate headings receive a
 * deterministic numeric suffix.
 *
 * @param {ParentNode} root
 * @returns {void}
 */
function addHeadingAnchors(root) {
  const headings = root.querySelectorAll(
    ".tab-pane h1:not([id]), .tab-pane h2:not([id]), .tab-pane h3:not([id]), " +
      ".tab-pane h4:not([id]), .tab-pane h5:not([id]), .tab-pane h6:not([id])"
  );

  headings.forEach((heading) => {
    const pane = heading.closest(".tab-pane");
    if (!(pane instanceof HTMLElement) || !pane.id) return;

    const text = (heading.textContent || "").trim();
    if (!text) return;

    const baseId = makeDeepLinkAnchorId(pane.id, text);
    let candidate = baseId;
    let suffix = 2;
    while (document.getElementById(candidate)) {
      candidate = `${baseId}-${suffix}`;
      suffix += 1;
    }

    heading.id = candidate;
    heading.classList.add("deep-link-anchor");
  });
}

/**
 * @returns {MutationObserver | null}
 */
function observeDynamicHeadings() {
  const tabContent = document.querySelector(".tab-content");
  if (!tabContent) return null;

  addHeadingAnchors(tabContent);
  const observer = new MutationObserver((mutations) => {
    if (mutations.some((mutation) => mutation.addedNodes.length > 0)) {
      addHeadingAnchors(tabContent);
    }
  });
  observer.observe(tabContent, { childList: true, subtree: true });
  return observer;
}

/**
 * Scroll to an anchor that may be created by an asynchronous table renderer.
 *
 * @param {string} hash
 * @returns {void}
 */
function scrollToAnchorWhenAvailable(hash) {
  if (!hash || hash === "#") return;

  let targetId;
  try {
    targetId = decodeURIComponent(hash.slice(1));
  } catch (_error) {
    targetId = hash.slice(1);
  }
  if (!targetId) return;

  const scrollIfPresent = () => {
    const target = document.getElementById(targetId);
    if (!target) return false;
    requestAnimationFrame(() => {
      requestAnimationFrame(() => target.scrollIntoView({ block: "start" }));
    });
    return true;
  };

  if (scrollIfPresent()) return;

  const observer = new MutationObserver(() => {
    addHeadingAnchors(document);
    if (scrollIfPresent()) observer.disconnect();
  });
  observer.observe(document.body, { childList: true, subtree: true });
  window.setTimeout(() => observer.disconnect(), 10000);
}

/**
 * @param {{slug: string, buttonId: string, paneId: string}} config
 * @returns {void}
 */
function replaceAddressForTab(config) {
  const url = new URL(window.location.href);
  url.searchParams.set("tab", config.slug);
  url.hash = "";

  if (config.slug !== "summary") {
    SUMMARY_PARAMETERS.forEach((parameter) => url.searchParams.delete(parameter));
  }

  window.history.replaceState(null, "", `${url.pathname}${url.search}`);
}

/**
 * Activate a tab requested by `?tab=...`, wait for an optional section hash,
 * and keep the address bar synchronized with later tab selections.
 *
 * @returns {void}
 */
export function initializeDeepLinks() {
  observeDynamicHeadings();

  const params = new URLSearchParams(window.location.search);
  let requested = resolveTab(params.get("tab"));
  const initialHash = window.location.hash;

  if (!requested && initialHash) {
    const targetAnchor = slugifyDeepLinkText(initialHash.slice(1));
    requested = Object.entries(DEEP_LINK_TABS)
      .map(([slug, config]) => ({ slug, ...config }))
      .find((config) => {
        const panePrefix = `${slugifyDeepLinkText(config.paneId)}-`;
        return targetAnchor === slugifyDeepLinkText(config.paneId) ||
          targetAnchor.startsWith(panePrefix);
      }) || null;
  }

  let suppressInitialUpdateFor = requested?.buttonId || null;

  for (const [slug, config] of Object.entries(DEEP_LINK_TABS)) {
    const button = document.getElementById(config.buttonId);
    if (!button) continue;

    button.addEventListener("shown.bs.tab", (event) => {
      if (suppressInitialUpdateFor === event.target.id) {
        suppressInitialUpdateFor = null;
        scrollToAnchorWhenAvailable(initialHash);
        return;
      }
      replaceAddressForTab({ slug, ...config });
    });
  }

  if (!requested) {
    if (params.has("tab")) {
      console.warn(`[deep-links] Unknown tab: ${params.get("tab")}`);
    }
    scrollToAnchorWhenAvailable(initialHash);
    return;
  }

  const requestedButton = document.getElementById(requested.buttonId);
  if (!(requestedButton instanceof HTMLElement)) {
    console.warn(`[deep-links] Tab control not found: ${requested.buttonId}`);
    return;
  }

  if (requestedButton.classList.contains("active")) {
    suppressInitialUpdateFor = null;
    scrollToAnchorWhenAvailable(initialHash);
  } else {
    requestedButton.click();
  }
}
