// glossary.js

/**
 * @typedef {{
 *   citation?: string,
 *   doi?: string,
 *   note?: string
 * }} GlossarySource
 */

/**
 * @typedef {{
 *   key: string,
 *   term: string,
 *   abbreviation?: string,
 *   definition: string,
 *   units?: string,
 *   matches?: string[],
 *   related_to?: string[],
 *   source?: GlossarySource
 * }} GlossaryEntry
 */

/**
 * @typedef {{
 *   key: string,
 *   label: string,
 *   items: GlossaryEntry[]
 * }} GlossarySection
 */

/**
 * @typedef {{
 *   sections: GlossarySection[]
 * }} GlossaryData
 */

/**
 * @typedef {Window & {
 *   bootstrap?: {
 *     Collapse?: new (el: Element, options?: object) => { show: () => void, hide: () => void },
 *     Tooltip?: {
 *       getOrCreateInstance: (el: Element) => unknown
 *     }
 *   },
 *   __glossaryData?: GlossaryData
 * }} GlossaryWindow
 */

/** @type {GlossaryWindow} */
const glossaryWindow = /** @type {GlossaryWindow} */ (window);

const GLOSSARY_JSON_URL = "/static/data/glossary_terms.json";

/**
 * @param {GlossaryEntry} entry
 * @returns {string}
 */
function glossaryDisplayTerm(entry) {
  return entry.abbreviation
    ? `${entry.term} (${entry.abbreviation})`
    : entry.term;
}

/**
 * @param {string} value
 * @returns {string}
 */
function normalizeSearchText(value) {
  return String(value || "").toLowerCase().trim();
}

/**
 * @param {GlossaryEntry} entry
 * @returns {string}
 */
function searchableEntryText(entry) {
  const parts = [
    entry.key,
    entry.term,
    entry.abbreviation || "",
    entry.definition || "",
    entry.units || "",
    ...(entry.matches || []),
    ...(entry.related_to || []),
    entry.source?.citation || "",
    entry.source?.doi || "",
    entry.source?.note || "",
  ];

  return normalizeSearchText(parts.join(" "));
}

function escapeRegex(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function clearHighlights(root) {
  root.querySelectorAll("mark.glossary-highlight").forEach((mark) => {
    const text = document.createTextNode(mark.textContent || "");
    mark.replaceWith(text);
  });
}

function highlightMatches(root, query) {
  if (!query) return;

  const regex = new RegExp(`(${escapeRegex(query)})`, "gi");

  root.querySelectorAll(".glossary-term, .glossary-definition, .glossary-units, .glossary-related, .glossary-source").forEach((el) => {
    if (!(el instanceof HTMLElement)) return;

    el.childNodes.forEach((node) => {
      if (node.nodeType !== Node.TEXT_NODE) return;

      const text = node.textContent || "";
      if (!regex.test(text)) return;

      regex.lastIndex = 0;

      const span = document.createElement("span");
      span.innerHTML = text.replace(regex, `<mark class="glossary-highlight">$1</mark>`);
      node.replaceWith(...Array.from(span.childNodes));
    });
  });
}

/**
 * @param {GlossaryEntry[]} allEntries
 * @param {string[] | undefined} relatedKeys
 * @returns {string}
 */
function relatedTermsText(allEntries, relatedKeys) {
  if (!relatedKeys || relatedKeys.length === 0) return "";

  const labels = relatedKeys.map((key) => {
    const match = allEntries.find((entry) => entry.key === key);
    return match ? glossaryDisplayTerm(match) : key;
  });

  return labels.join(", ");
}

/**
 * @param {HTMLElement} container
 * @param {boolean} show
 */
function setEntryVisible(container, show) {
  container.classList.toggle("d-none", !show);
}

/**
 * @returns {Promise<GlossaryData>}
 */
export async function loadGlossaryData() {
  if (glossaryWindow.__glossaryData) {
    return glossaryWindow.__glossaryData;
  }

  const resp = await fetch(GLOSSARY_JSON_URL);
  if (!resp.ok) {
    throw new Error(`Failed to load glossary JSON: HTTP ${resp.status}`);
  }

  const data = await resp.json();
  glossaryWindow.__glossaryData = data;
  return data;
}

/**
 * @returns {Promise<void>}
 */
export async function renderGlossary() {
  const container = document.getElementById("glossary-content");
  if (!container) {
    console.warn("Glossary container #glossary-content not found.");
    return;
  }

  if (container.dataset.rendered === "true") return;

  const glossaryData = await loadGlossaryData();
  const allEntries = glossaryData.sections.flatMap((section) => section.items || []);

  container.innerHTML = "";

  const intro = document.createElement("div");
  intro.className = "mb-3";
intro.innerHTML = `
  <p class="mb-2">
    This glossary defines terms used throughout the biochar research website, including field measurements,
    soil chemistry, soil biology, irrigation, forage quality, and app-specific analysis terms.
  </p>
  <p class="mb-2">
    <strong>Note:</strong> Many laboratory-test definitions are adapted from Ward Laboratories guidance documents:
  </p>
  <ul class="mb-2">
    <li><a href="https://www.wardlab.com/wp-content/uploads/2024/04/2024-Soil-Health-One-Pager-C.pdf" target="_blank">Ward Soil Health Overview</a></li>
    <li><a href="https://www.wardlab.com/wp-content/uploads/2024/12/SHA-Guide-FINAL-May.pdf" target="_blank">Ward Soil Health Assessment Guide</a></li>
  </ul>
  <p class="mb-2">
    Units and interpretation notes follow Ward terminology where available. In cases where Ward documentation
    describes a method without specifying an explicit unit, the glossary reflects the best available interpretation.
  </p>
  `;
  container.appendChild(intro);

  const searchWrap = document.createElement("div");
  searchWrap.className = "mb-3";
  searchWrap.innerHTML = `
    <label for="glossary-search" class="form-label fw-semibold">Search glossary</label>
    <input
      id="glossary-search"
      type="search"
      class="form-control"
      placeholder="Search terms, definitions, units, abbreviations, or column names..."
      autocomplete="off"
    >
    <div id="glossary-search-status" class="form-text"></div>
  `;
  container.appendChild(searchWrap);

  const accordion = document.createElement("div");
  accordion.className = "accordion";
  accordion.id = "glossary-accordion";

  glossaryData.sections.forEach((section, index) => {
    const item = document.createElement("div");
    item.className = "accordion-item glossary-section";
    item.dataset.sectionKey = section.key;

    const headerId = `glossary-heading-${section.key}`;
    const collapseId = `glossary-collapse-${section.key}`;
    const isFirst = index === 0;

    item.innerHTML = `
      <h2 class="accordion-header" id="${headerId}">
        <button
          class="accordion-button ${isFirst ? "" : "collapsed"}"
          type="button"
          data-bs-toggle="collapse"
          data-bs-target="#${collapseId}"
          aria-expanded="${isFirst ? "true" : "false"}"
          aria-controls="${collapseId}">
          ${section.label}
        </button>
      </h2>
      <div
        id="${collapseId}"
        class="accordion-collapse collapse ${isFirst ? "show" : ""}"
        aria-labelledby="${headerId}">
        <div class="accordion-body"></div>
      </div>
    `;

    const body = item.querySelector(".accordion-body");
    if (body instanceof HTMLElement) {
      section.items.forEach((entry) => {
        const termBlock = document.createElement("div");
        termBlock.className = "mb-3 glossary-entry";
        termBlock.dataset.searchText = searchableEntryText(entry);

        const relatedText = relatedTermsText(allEntries, entry.related_to);

        const unitsHtml = entry.units
          ? `<div class="glossary-units small"><strong>Units:</strong> ${entry.units}</div>`
          : "";

        const relatedHtml = relatedText
          ? `<div class="glossary-related small"><strong>Related terms:</strong> ${relatedText}</div>`
          : "";

        const sourceParts = [];
        if (entry.source?.citation) sourceParts.push(entry.source.citation);
        if (entry.source?.doi) sourceParts.push(`DOI: ${entry.source.doi}`);
        if (entry.source?.note) sourceParts.push(entry.source.note);

        const sourceHtml = sourceParts.length
          ? `<div class="glossary-source small"><strong>Source:</strong> ${sourceParts.join("; ")}</div>`
          : "";

        termBlock.innerHTML = `
          <div class="glossary-term fw-semibold">${glossaryDisplayTerm(entry)}</div>
          <div class="glossary-definition">${entry.definition || ""}</div>
          ${unitsHtml}
          ${relatedHtml}
          ${sourceHtml}
        `;

        body.appendChild(termBlock);
      });
    }

    accordion.appendChild(item);
  });

  container.appendChild(accordion);

  const searchInput = document.getElementById("glossary-search");
  const status = document.getElementById("glossary-search-status");

  if (searchInput instanceof HTMLInputElement) {
    searchInput.addEventListener("input", () => {
      const query = normalizeSearchText(searchInput.value);
      clearHighlights(container);
      const sections = Array.from(container.querySelectorAll(".glossary-section"));
      let totalMatches = 0;

      sections.forEach((sectionEl) => {
        if (!(sectionEl instanceof HTMLElement)) return;

        const entries = Array.from(sectionEl.querySelectorAll(".glossary-entry"));
        let sectionMatches = 0;

        entries.forEach((entryEl) => {
          if (!(entryEl instanceof HTMLElement)) return;

          const entryText = entryEl.dataset.searchText || "";
          const isMatch = !query || entryText.includes(query);

          setEntryVisible(entryEl, isMatch);

          if (isMatch) {
            sectionMatches += 1;
            totalMatches += 1;
          }
        });

        const collapseEl = sectionEl.querySelector(".accordion-collapse");
        const buttonEl = sectionEl.querySelector(".accordion-button");

        if (collapseEl instanceof HTMLElement && buttonEl instanceof HTMLElement) {
          if (query && sectionMatches > 0) {
            const collapse = glossaryWindow.bootstrap?.Collapse
              ? new glossaryWindow.bootstrap.Collapse(collapseEl, { toggle: false })
              : null;

            collapse?.show();
            collapseEl.classList.add("show");
            buttonEl.classList.remove("collapsed");
            buttonEl.setAttribute("aria-expanded", "true");
          } else if (query && sectionMatches === 0) {
            const collapse = glossaryWindow.bootstrap?.Collapse
              ? new glossaryWindow.bootstrap.Collapse(collapseEl, { toggle: false })
              : null;

            collapse?.hide();
            collapseEl.classList.remove("show");
            buttonEl.classList.add("collapsed");
            buttonEl.setAttribute("aria-expanded", "false");
          }
        }

        sectionEl.classList.toggle("d-none", query.length > 0 && sectionMatches === 0);
      });

      if (query) {
        highlightMatches(container, query);
      }

      if (status instanceof HTMLElement) {
        if (!query) {
          status.textContent = "";
        } else if (totalMatches === 1) {
          status.textContent = "1 matching glossary entry";
        } else {
          status.textContent = `${totalMatches} matching glossary entries`;
        }
      }
    });
  }

  container.dataset.rendered = "true";
}

/**
 * @returns {Promise<Record<string, string>>}
 */
export async function buildGlossaryLookup() {
  const glossaryData = await loadGlossaryData();

  /** @type {Record<string, string>} */
  const lookup = {};

  glossaryData.sections.forEach((section) => {
    section.items.forEach((entry) => {
      lookup[entry.term] = entry.definition;
      lookup[glossaryDisplayTerm(entry)] = entry.definition;

      if (entry.abbreviation) {
        lookup[entry.abbreviation] = entry.definition;
      }

      (entry.matches || []).forEach((match) => {
        lookup[match] = entry.definition;
      });
    });
  });

  return lookup;
}

/**
 * @param {ParentNode} [root=document]
 * @returns {Promise<void>}
 */
export async function applyGlossaryTooltips(root = document) {
  const lookup = await buildGlossaryLookup();

  const nodes = root.querySelectorAll("[data-glossary-term]");

  nodes.forEach((node) => {
    const term = node.getAttribute("data-glossary-term");
    if (!term) return;

    const definition = lookup[term];
    if (!definition) return;

    node.setAttribute("title", definition);
    node.setAttribute("data-bs-toggle", "tooltip");
    node.setAttribute("data-bs-placement", "top");
    node.setAttribute("data-bs-trigger", "hover focus");
  });

  const TooltipCtor = glossaryWindow.bootstrap?.Tooltip;
  if (TooltipCtor) {
    root.querySelectorAll('[data-bs-toggle="tooltip"]').forEach((el) => {
      TooltipCtor.getOrCreateInstance(el);
    });
  }
}