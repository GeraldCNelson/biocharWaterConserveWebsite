// biomass_field_tab.js
import { fetchJson } from "./api_requests.js";
import { normalizePayload, renderOneSetFromPayload } from "./tables.js";
import { makeSetSectionTitle } from "./tab_ui.js";

export async function renderBiomassFieldTables() {
  const container = document.getElementById("biomass-field-content");
  if (!container) {
    console.warn("Biomass Field container #biomass-field-content not found.");
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
    const rawPayload = await fetchJson("/api/get_biomass_field_table");
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

    // Usually only 1 set, but keep this generic
    for (const setPayload of payload.sets) {
      const label = setPayload.label || "Biomass (Field Samples)";

      // One section per set
      const section = makeSetSectionTitle(
        label,
        "Rows: field locations (e.g., S1M, S1B). Columns: sampling dates. Values are dry biomass (g).",
        "nir"
      );
      container.appendChild(section);

      // Render table content into the section
      renderOneSetFromPayload(section, setPayload);
    }

    container.dataset.rendered = "true";
  } catch (err) {
    console.error("Failed to render Biomass Field tables:", err);

    // If loading is already removed, this is safe (no-op)
    loading.remove();

    const div = document.createElement("div");
    div.className = "alert alert-danger";
    div.textContent = "Failed to load Biomass Field tables. Check server logs.";
    container.appendChild(div);
  }
}