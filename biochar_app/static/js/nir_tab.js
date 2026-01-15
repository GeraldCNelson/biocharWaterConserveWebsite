// biochar_app/static/js/nir_tab.js

let _nirSet1LoadedOnce = false;

/**
 * Render the NIR Set 1 table into the NIR tab, typically called when the tab activates.
 *
 * Options:
 *  - forceReload: if true, fetch again even if previously loaded
 */
export async function renderNirSet1Table({ forceReload = false } = {}) {
  const container = document.getElementById("nir-set1-table-container");
  if (!container) {
    console.warn("NIR container not found: #nir-set1-table-container");
    return;
  }

  if (_nirSet1LoadedOnce && !forceReload) return;

  container.innerHTML = `
    <div class="text-muted">
      Loading NIR Clean Data (Set 1)…
    </div>
  `;

  try {
    const resp = await fetch("/api/get_nir_set1_table", {
      method: "GET",
      headers: { "Accept": "application/json" },
    });

    if (!resp.ok) {
      const txt = await resp.text();
      throw new Error(`GET /api/get_nir_set1_table failed (${resp.status}): ${txt}`);
    }

    const payload = await resp.json();

    // --- Preferred: backend supplies ready-to-insert HTML ---
    const html = payload?.html ?? payload?.table_html ?? payload?.tableHtml;
    if (typeof html === "string" && html.trim().length > 0) {
      container.innerHTML = html;
      _nirSet1LoadedOnce = true;
      return;
    }

    // --- Otherwise: try a generic "columns + rows" shape ---
    const cols =
      payload?.columns ??
      payload?.headers ??
      payload?.colnames ??
      payload?.colNames ??
      null;

    const rows =
      payload?.rows ??
      payload?.data ??
      payload?.values ??
      null;

    if (Array.isArray(cols) && Array.isArray(rows)) {
      container.innerHTML = buildHtmlTable(cols, rows);
      _nirSet1LoadedOnce = true;
      return;
    }

    // Fallback: show payload for debugging
    container.innerHTML = `
      <div class="alert alert-warning">
        NIR table endpoint returned JSON but not in a recognized shape.
        <pre class="mb-0" style="white-space: pre-wrap;">${escapeHtml(JSON.stringify(payload, null, 2))}</pre>
      </div>
    `;
  } catch (err) {
    console.error(err);
    container.innerHTML = `
      <div class="alert alert-danger">
        Failed to load NIR table. Check server logs and the Network tab.
        <div class="mt-2"><code>${escapeHtml(String(err))}</code></div>
      </div>
    `;
  }
}

function buildHtmlTable(columns, rows) {
  const thead = `
    <thead>
      <tr>
        ${columns.map(c => `<th scope="col">${escapeHtml(String(c))}</th>`).join("")}
      </tr>
    </thead>
  `;

  const tbody = `
    <tbody>
      ${rows.map(r => {
        // r can be an array, or an object keyed by column names
        const cells = Array.isArray(r)
          ? columns.map((_, i) => r[i])
          : columns.map(c => r?.[c]);

        return `<tr>${cells.map(v => `<td>${escapeHtml(v ?? "")}</td>`).join("")}</tr>`;
      }).join("")}
    </tbody>
  `;

  return `
    <div class="table-responsive">
      <table class="table table-sm table-striped table-bordered align-middle">
        ${thead}
        ${tbody}
      </table>
    </div>
  `;
}

function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}