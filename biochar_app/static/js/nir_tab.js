// static/js/nir_tab.js
//
// Renderer for the Pasture Quality Metrics tab.
//
// Expected (preferred) payload shape:
// {
//   title: "Pasture Quality Metrics",
//   sets: [
//     {
//       key: "nir_set1",
//       label: "Pasture Quality Metrics — Set 1",
//       periods: [{key,label}, ...]  OR ["2023-05-01", ...],
//       variables: [{key,label}, ...] OR ["cp_pct", ...],
//       rows: ["strip_1", ...],
//       rowLabels: {"strip_1":"STRIP 1", ...},
//       data: { varKey: { rowKey: { periodKey: value|null } } }
//     },
//     ...
//   ]
// }
//
// Also tolerates legacy / single-set payloads where the top-level contains
// periods/variables/rows/rowLabels/data directly. We normalize via tables.js.

import { makeSetSectionTitle } from "./tab_ui.js";
import { normalizePayload, renderOneSetFromPayload } from "./tables.js";
import { fetchJson } from "./api_requests.js";

/**
 * Public: render NIR tables into the NIR tab.
 * Endpoint:
 *   GET /api/get_nir_table
 */
export async function renderNirTables() {
  const container = document.getElementById("nir-content");
  if (!container) {
    console.warn("[renderNirTables] Container #nir-content not found.");
    return;
  }

  // Prevent double-render if user clicks the tab repeatedly
  if (container.dataset.rendered === "true") return;

  container.innerHTML = "";

  const loading = document.createElement("div");
  loading.className = "text-muted";
  loading.textContent = "Loading…";
  container.appendChild(loading);

  try {
    const rawPayload = await fetchJson("/api/get_nir_table");
    const payload = normalizePayload(rawPayload);

    loading.remove();

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
      const label = setPayload?.label || setPayload?.key || "Set";
      const section = makeSetSectionTitle(label);
      container.appendChild(section);

      // tables.js handles rendering the table (and any optional setPayload.note)
      renderOneSetFromPayload(section, setPayload);
    }

    container.dataset.rendered = "true";
  } catch (err) {
    console.error("Failed to render NIR tables:", err);
    loading.remove();

    const div = document.createElement("div");
    div.className = "alert alert-danger";
    div.textContent = "Failed to load Pasture Quality Metrics tables. Check server and browser logs.";
    container.appendChild(div);
  }
}