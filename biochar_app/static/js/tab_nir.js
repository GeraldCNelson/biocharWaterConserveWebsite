// static/js/tab_nir.js
//
// Renderer for the Pasture Quality Metrics tab.
// Expects endpoint:
//   GET /api/get_nir_table
//
// Payload supported:
// - Standard multi-set: { title, sets: [ ... ] }
// - Legacy single-set:  { title, periods, variables, rows, rowLabels, data }

import { makeSetSectionTitle } from "./tab_ui.js";
import { normalizePayload, renderOneSetFromPayload } from "./tables.js";
import { fetchJson } from "./api_requests.js";
import { showLoadingOverlay, hideLoadingOverlay, startLoadingDots, stopLoadingDots } from "./ui_loading.js";

export async function renderNirTables() {
  const container = document.getElementById("nir-content");
  if (!container) {
    console.warn("NIR container #nir-content not found.");
    return;
  }

  // Prevent double-render if the user clicks tabs repeatedly
  if (container.dataset.rendered === "true") return;

  container.innerHTML = "";

  const loading = document.createElement("div");
  loading.className = "text-muted";
  loading.textContent = "Loading…";
  container.appendChild(loading);

  try {
    const rawPayload = await fetchJson("/api/get_nir_table");
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
      const section = makeSetSectionTitle(setPayload.label);
      container.appendChild(section);
      renderOneSetFromPayload(section, setPayload);
    }

    container.dataset.rendered = "true";
  } catch (err) {
    console.error("Failed to render NIR tables:", err);
    loading.remove();

    const div = document.createElement("div");
    div.className = "alert alert-danger";
    div.textContent = "Failed to load NIR tables. Check server logs.";
    container.appendChild(div);
  }
}