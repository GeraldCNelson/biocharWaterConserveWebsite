// static/js/soil_tab.js
//
// Simple renderer for Soil Chemistry + Soil Biological Health tabs.
// Uses shared helpers in tables.js so Soil/NIR/Biomass all behave consistently.

import { makeSetSectionTitle } from "./tab_ui.js";
import { normalizePayload, renderOneSetFromPayload, safeStr } from "./tables.js";
import { fetchJson } from "./api_requests.js";
import { showLoadingOverlay, hideLoadingOverlay, startLoadingDots, stopLoadingDots } from "./ui_loading.js";

async function renderSoilTab({
  containerId,
  endpoint,
  fallbackLabel,
  subtitleText = "",
}) {
  const container = document.getElementById(containerId);
  if (!container) {
    console.warn(`Soil container #${containerId} not found.`);
    return;
  }

  // Prevent double-render if the user clicks tabs repeatedly
  if (container.dataset.rendered === "true") return;

  // ✅ Use the shared animated overlay (prevents static “Loading…” text)
  // NOTE: keep this import at the top of soil_tab.js:
  // import { showLoadingOverlay, hideLoadingOverlay } from "./ui_loading.js";
  showLoadingOverlay(container, "Loading");
  container.innerHTML = ""; // safe: overlay is appended after we call showLoadingOverlay()

  try {
    const rawPayload = await fetchJson(endpoint);

    const payload = normalizePayload(rawPayload);

    if (!payload || !Array.isArray(payload.sets) || payload.sets.length === 0) {
      const pre = document.createElement("pre");
      pre.className = "alert alert-warning";
      pre.textContent =
        "Endpoint returned JSON but not in a recognized shape.\n\n" +
        JSON.stringify(rawPayload, null, 2);
      container.appendChild(pre);
      return;
    }

    for (const setPayload of payload.sets) {
      const label = setPayload.label || fallbackLabel;

      // Prefer setPayload.notes if present; else fallback
      const note = setPayload.notes || setPayload.note || subtitleText || "";

      // --- Create a per-set wrapper so each set is clearly separated ---
      const setWrapper = document.createElement("div");
      setWrapper.className = "mb-4";
      container.appendChild(setWrapper);

      // Title (and note)
      const titleEl = makeSetSectionTitle(label, note, "soil");
      setWrapper.appendChild(titleEl);

      // Body container for the tables
      const bodyEl = document.createElement("div");
      bodyEl.className = "soil-set-body";
      setWrapper.appendChild(bodyEl);

      // Render tables INTO the body, not into the title element
      renderOneSetFromPayload(bodyEl, setPayload);
    }

    container.dataset.rendered = "true";
  } catch (err) {
    console.error(`Failed to render ${containerId}:`, err);

    const div = document.createElement("div");
    div.className = "alert alert-danger";
    div.textContent = "Failed to load tables. Check server logs.";
    container.appendChild(div);
  } finally {
    // ✅ Always clear overlay
    hideLoadingOverlay(container);
  }
}

// Public entry points used by main.js
export async function renderSoilChemTable() {
  return renderSoilTab({
    containerId: "soilchem-content",
    endpoint: "/api/get_soilchem_table",
    fallbackLabel: "Soil Chemistry",
  });
}

export async function renderSoilBioTable() {
  return renderSoilTab({
    containerId: "soilbio-content",
    endpoint: "/api/get_soilbio_table",
    fallbackLabel: "Soil Biological Health",
  });
}