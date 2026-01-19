// static/js/soil_tab.js
import { debugLog } from "./plots.js";

/**
 * Reuse your existing NIR renderer if you want, but simplest is:
 * build a basic HTML table and inject it into the container.
 *
 * Payload shape (matches soil_tables.py):
 * {
 *   periods: [{key,label}, ...],
 *   variables: [{key,label}, ...],
 *   rows: ["strip_1","strip_2","strip_3","strip_4","s1_s2","s3_s4"],
 *   data: { varKey: { rowKey: { periodKey: value|null } } }
 * }
 */

function formatValue(v) {
  if (v === null || v === undefined || Number.isNaN(v)) return "";
  if (typeof v === "number") return v.toFixed(3);
  const n = Number(v);
  return Number.isFinite(n) ? n.toFixed(3) : String(v);
}

function buildHtmlTable(payload, titleText) {
  const periods = payload.periods || [];
  const variables = payload.variables || [];
  const rows = payload.rows || [];
  const data = payload.data || {};

  if (!periods.length || !variables.length) {
    return `<div class="text-muted">${titleText}: no sampling events found.</div>`;
  }

  // Build one table per variable (Set1 is small by design)
  const tables = variables.map((v) => {
    const varKey = v.key;
    const varLabel = v.label || varKey;
    const grid = data[varKey] || {};

    const thead =
      `<thead><tr>` +
      `<th style="position:sticky;left:0;background:white;z-index:2;">Row</th>` +
      periods.map((p) => `<th>${p.label}</th>`).join("") +
      `</tr></thead>`;

    const tbody =
      `<tbody>` +
      rows
        .map((r) => {
          const rowMap = grid[r] || {};
          const tds =
            `<td style="position:sticky;left:0;background:white;z-index:1;"><b>${r}</b></td>` +
            periods
              .map((p) => `<td>${formatValue(rowMap[p.key])}</td>`)
              .join("");
          return `<tr>${tds}</tr>`;
        })
        .join("") +
      `</tbody>`;

    return `
      <div class="mb-4">
        <h5 class="mb-2">${varLabel}</h5>
        <div class="table-responsive">
          <table class="table table-sm table-striped table-bordered align-middle">
            ${thead}
            ${tbody}
          </table>
        </div>
      </div>
    `;
  });

  return tables.join("\n");
}

async function fetchJson(url) {
  const res = await fetch(url, { headers: { Accept: "application/json" } });
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`${url} failed: ${res.status} ${res.statusText} — ${txt}`);
  }
  return await res.json();
}

export async function renderSoilChemTable() {
  const container = document.getElementById("soilchem-content");
  if (!container) return;

  container.innerHTML = `<div class="text-muted">Loading soil chemistry…</div>`;
  try {
    const payload = await fetchJson("/api/get_soilchem_set1_table");
    container.innerHTML = buildHtmlTable(payload, "Soil chemistry");
    debugLog("✅ Soil chemistry table rendered");
  } catch (err) {
    console.error("❌ renderSoilChemTable failed:", err);
    container.innerHTML = `<div class="text-danger">Failed to load soil chemistry table.</div>`;
  }
}

export async function renderSoilBioTable() {
  const container = document.getElementById("soilbio-content");
  if (!container) return;

  container.innerHTML = `<div class="text-muted">Loading soil biological health…</div>`;
  try {
    const payload = await fetchJson("/api/get_soilbio_set1_table");
    container.innerHTML = buildHtmlTable(payload, "Soil biological health");
    debugLog("✅ Soil biological health table rendered");
  } catch (err) {
    console.error("❌ renderSoilBioTable failed:", err);
    container.innerHTML = `<div class="text-danger">Failed to load soil biological health table.</div>`;
  }
}