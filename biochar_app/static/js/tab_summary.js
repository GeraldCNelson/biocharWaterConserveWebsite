// static/js/summary_tab.js
//
// Summary Statistics tab controller:
// - Fetch /api/get_summary_stats
// - Render raw + ratio tables using generateSummaryTable()
// - Store window.latestSummaryStats for downloads, etc.

import { fetchJson } from "./api_requests.js";
import { generateSummaryTable } from "./api_requests.js";
import { getDropdownValue } from "./ui_utils.js";

// If you don’t currently export getDropdownValue from ui_utils.js,
// either export it there OR replace calls below with direct document.getElementById(...).value.

function getUnitSystemForSummary() {
  const toggle = document.getElementById("units-toggle_summary");
  return toggle && toggle.checked ? "metric" : "us";
}

export async function updateSummaryStatistics() {
  console.log("📊 updateSummaryStatistics: Updating summary statistics...");

  const container = document.getElementById("summary-table-container");
  if (!container) {
    console.warn("❌ #summary-table-container not found.");
    return;
  }

  // show loading immediately
  container.innerHTML = "";
  const loading = document.createElement("div");
  loading.className = "text-muted";
  loading.textContent = "Loading…";
  container.appendChild(loading);

  try {
    const year = parseInt(getDropdownValue("summary-year"), 10);
    const variable = getDropdownValue("summary-variable");
    const stripRaw = getDropdownValue("summary-strip");
    const strip = stripRaw ? stripRaw : null;
    const granularity = getDropdownValue("summary-granularity");
    const depthRaw = getDropdownValue("summary-depth");
    const depth = depthRaw ? depthRaw : null;
    const unitSystem = getUnitSystemForSummary();

    console.log("🔍 Selected Summary Filters:", { year, variable, strip, granularity, depth, unitSystem });

    // If your backend requires strip too, add `|| !strip`
    if (!Number.isFinite(year) || !variable || !granularity || !depth) {
      loading.remove();
      const warn = document.createElement("div");
      warn.className = "alert alert-warning";
      warn.textContent = "Please select Year, Variable, Time averages, and Depth before updating the summary.";
      container.appendChild(warn);
      return;
    }

    const data = await fetchJson("/api/get_summary_stats", {
      method: "POST",
      body: JSON.stringify({ year, variable, strip, granularity, depth, unitSystem })
    });

    console.log("✅ Received summary stats response:", data);

    // Cache for downloads, debugging
    window.latestSummaryStats = {
      raw: data?.raw_statistics ?? null,
      ratio: data?.ratio_statistics ?? null,
      meta: { year, variable, strip, granularity, depth, unitSystem }
    };

    // Title
    const titleEl = document.getElementById("summary-title");
    if (titleEl) titleEl.textContent = data?.title || "Summary Results";

    // Clear loading and render
    container.innerHTML = "";

    // RAW section
    const rawHeader = document.createElement("h5");
    rawHeader.textContent = "Raw Data";
    container.appendChild(rawHeader);

    const rawTableEl = generateSummaryTable(data?.raw_statistics, variable);
    console.log("rawTableEl type:", typeof rawTableEl, rawTableEl);

    if (rawTableEl instanceof Node) {
      container.appendChild(rawTableEl);
    } else {
      const warn = document.createElement("div");
      warn.className = "alert alert-warning";
      warn.textContent = "Raw summary table could not be rendered (unexpected return type).";
      container.appendChild(warn);
    }

    // RATIO section only if present
    const ratioStats = data?.ratio_statistics;
    if (ratioStats && typeof ratioStats === "object") {
      const ratioHeader = document.createElement("h5");
      ratioHeader.className = "mt-4";
      ratioHeader.textContent = "Ratio Data";
      console.log("rawTableEl type:", typeof rawTableEl, rawTableEl);
      container.appendChild(ratioHeader);
      container.appendChild(generateSummaryTable(ratioStats, variable));
    }

    console.log("✅ Summary statistics tables updated.");
  } catch (error) {
    console.error("❌ Unexpected error in updateSummaryStatistics:", error);

    container.innerHTML = "";
    const div = document.createElement("div");
    div.className = "alert alert-danger";
    div.textContent = "Failed to load summary statistics. Check server logs and browser console.";
    container.appendChild(div);
  }
}

// static/js/tab_summary.js

export function initSummaryTab() {
  const btn = document.getElementById("update-summary");

  if (!btn) {
    console.warn("⚠️ #update-summary button not found; summary tab not wired.");
    return;
  }

  btn.addEventListener("click", (e) => {
    e.preventDefault();
    updateSummaryStatistics();
  });

  console.debug("✅ Summary tab wired (update button)");
}