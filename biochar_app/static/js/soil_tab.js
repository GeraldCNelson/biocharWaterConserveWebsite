// static/js/soil_tab.js
//
// Simple renderer for Soil Chemistry + Soil Biological Health tabs.
// Uses shared helpers in tables.js so Soil/NIR/Biomass all behave consistently.

import { makeSetSectionTitle } from "./tab_ui.js";
import { normalizePayload, renderOneSetFromPayload } from "./tables.js";
import { fetchJson } from "./api_requests.js";

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

  container.innerHTML = "";

  // Loading indicator
  const loading = document.createElement("div");
  loading.className = "text-muted";
  loading.textContent = "Loading…";
  container.appendChild(loading);

  try {
    const rawPayload = await fetchJson(endpoint);
    loading.remove();

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
      const note = setPayload.note || subtitleText;

      // variant="soil" -> uses soil-* CSS classes
      const section = makeSetSectionTitle(label, note, "soil");
      container.appendChild(section);

      renderOneSetFromPayload(section, setPayload);
    }

    container.dataset.rendered = "true";
  } catch (err) {
    console.error(`Failed to render ${containerId}:`, err);
    loading.remove();

    const div = document.createElement("div");
    div.className = "alert alert-danger";
    div.textContent = "Failed to load tables. Check server logs.";
    container.appendChild(div);
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