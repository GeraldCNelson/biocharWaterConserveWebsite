// static/js/tables.js
//
// Shared helpers for rendering table payloads.
// This file contains:
// - isObject, safeStr
// - normalizePayload (supports multi-set and legacy single-set)
// - renderOneSetFromPayload
// - buildTableForVariable
//
// IMPORTANT FIX (Jan 2026):
// normalizeOneSet must preserve {key,label} objects for periods/variables.
// Converting them to strings breaks renderOneSetFromPayload (expects v.key / p.key).

export function isObject(x) {
  return x !== null && typeof x === "object" && !Array.isArray(x);
}

export function safeStr(v, fallback = "") {
  if (v === null || v === undefined) return fallback;
  const s = String(v).trim();
  return s.length ? s : fallback;
}

// ------------------------------------------------------------
// Normalization
// ------------------------------------------------------------

function normalizeOneSet(s, idx = 0) {
  const set = isObject(s) ? s : {};

  // Accept either "label" or "title" as the display label
  const label =
    safeStr(set.label, "") ||
    safeStr(set.title, "") ||
    `Set ${idx + 1}`;

  const key = safeStr(set.key, "") || `set_${idx + 1}`;

  // Normalize {key,label} items (periods/variables).
  // - If entry is a string/number, treat it as both key and label.
  // - If entry is an object, preserve it but ensure it has key/label.
  function normalizeKeyLabelItem(x) {
    if (x === null || x === undefined) return null;

    if (typeof x === "string" || typeof x === "number") {
      const s = String(x);
      return { key: s, label: s };
    }

    if (isObject(x)) {
      const k = safeStr(x.key, "") || safeStr(x.id, "") || safeStr(x.name, "");
      const lbl = safeStr(x.label, "") || safeStr(x.title, "") || k;

      if (!k && !lbl) return null;

      // Preserve any extra fields (e.g., note, unit, etc.)
      return { ...x, key: k || lbl, label: lbl || k };
    }

    return null;
  }

  function normalizeKeyLabelList(arr) {
    if (!Array.isArray(arr)) return [];
    return arr
      .map(normalizeKeyLabelItem)
      .filter((v) => v && v.key);
  }

  const periods = normalizeKeyLabelList(set.periods);
  const variables = normalizeKeyLabelList(set.variables);
  const rows = Array.isArray(set.rows) ? set.rows.map(String) : [];

  const rowLabels = isObject(set.rowLabels) ? set.rowLabels : {};
  const data = isObject(set.data) ? set.data : {};

  // IMPORTANT: support both "note" and "notes" from backend
  const note = safeStr(set.note, "") || safeStr(set.notes, "");

  return {
    key,
    label,
    periods,
    variables,
    rows,
    rowLabels,
    data,
    note,
  };
}

/**
 * normalizePayload supports BOTH:
 *  A) Standard multi-set: { title, sets: [ {key,label,periods,variables,rows,rowLabels,data,note/notes?}, ... ] }
 *  B) Legacy single-set:  { title, periods, variables, rows, rowLabels, data, note/notes? }
 */
export function normalizePayload(raw) {
  if (!isObject(raw)) return { title: "", sets: [] };

  // Multi-set (preferred)
  if (Array.isArray(raw.sets)) {
    return {
      title: safeStr(raw.title, ""),
      sets: raw.sets.map((s, i) => normalizeOneSet(s, i)),
    };
  }

  // Legacy single-set payload
  const looksSingleSet =
    Array.isArray(raw.periods) &&
    Array.isArray(raw.variables) &&
    Array.isArray(raw.rows) &&
    isObject(raw.data);

  if (looksSingleSet) {
    const set = normalizeOneSet(
      {
        key: "set_1",
        label: safeStr(raw.title, "Set 1"),
        periods: raw.periods,
        variables: raw.variables,
        rows: raw.rows,
        rowLabels: raw.rowLabels,
        data: raw.data,
        note: safeStr(raw.note, "") || safeStr(raw.notes, ""),
      },
      0
    );

    return { title: safeStr(raw.title, ""), sets: [set] };
  }

  // Unknown shape
  return { title: safeStr(raw.title, ""), sets: [] };
}

// ------------------------------------------------------------
// Rendering helpers
// ------------------------------------------------------------

export function renderOneSetFromPayload(parentEl, setPayload) {
  if (!parentEl) return;

  const periods = Array.isArray(setPayload.periods) ? setPayload.periods : [];
  const variables = Array.isArray(setPayload.variables) ? setPayload.variables : [];
  const rows = Array.isArray(setPayload.rows) ? setPayload.rows : [];
  const rowLabels = isObject(setPayload.rowLabels) ? setPayload.rowLabels : {};
  const data = isObject(setPayload.data) ? setPayload.data : {};

  // NOTE:
  // We intentionally DO NOT inject set-level notes here anymore,
  // because the tab renderer can place the shared note once at the top.
  // (This prevents duplicate "Rows: ... Columns: ..." text.)

  // STRICT: variables must be provided by backend (or by set builder).
  if (variables.length === 0) {
    const dataKeys = isObject(data) ? Object.keys(data) : [];
    const diag = {
      setKey: setPayload.key,
      setLabel: setPayload.label,
      hasPeriods: periods.length,
      hasRows: rows.length,
      hasRowLabels: isObject(rowLabels) ? Object.keys(rowLabels).length : 0,
      hasVariables: variables.length,
      dataKeysSample: dataKeys.slice(0, 25),
      dataKeysCount: dataKeys.length,
      variableShape: setPayload.variables,
    };

    console.error("[tables.js] ❌ Missing variables[] for set; cannot render tables.", diag);

    const warn = document.createElement("div");
    warn.className = "alert alert-warning";
    warn.innerHTML =
      "<div><strong>No variables available for this table set.</strong></div>" +
      "<div class='mt-2'>This usually means the backend payload did not include <code>variables</code>.</div>" +
      "<div class='mt-2'><strong>Diagnostics:</strong></div>" +
      "<pre class='mb-0' style='white-space:pre-wrap'>" +
      escapeHtml(JSON.stringify(diag, null, 2)) +
      "</pre>";
    parentEl.appendChild(warn);
    return;
  }

  for (const v of variables) {
    const varKey = safeStr(v.key, "");
    const varLabel = safeStr(v.label, varKey);
    if (!varKey) continue;

    const tableEl = buildTableForVariable(
      { label: setPayload.label, periods, rows, rowLabels, data },
      varKey,
      varLabel,
      safeStr(v.note, "") // pass variable note through
    );

    parentEl.appendChild(tableEl);
  }
}

// Small helper for safe inline JSON display in HTML warnings.
function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

/**
 * Build a single variable table.
 * Expects:
 *   periods: [{key,label}, ...]
 *   rows: [rowKey, ...]
 *   rowLabels: {rowKey: label}
 *   data: { varKey: { rowKey: { periodKey: value|null } } }
 */
export function buildTableForVariable(setPayload, variableKey, variableLabel, variableNote = "") {
  const periods = Array.isArray(setPayload.periods) ? setPayload.periods : [];
  const rows = Array.isArray(setPayload.rows) ? setPayload.rows : [];
  const rowLabels = isObject(setPayload.rowLabels) ? setPayload.rowLabels : {};
  const data = isObject(setPayload.data) ? setPayload.data : {};

  const varBlock = data[variableKey];
  if (!isObject(varBlock)) {
    const div = document.createElement("div");
    div.className = "alert alert-warning";
    div.textContent = `No data found for variable: ${variableLabel || variableKey}`;
    return div;
  }

  // ----------------------------
  // Helpers
  // ----------------------------
  const norm = (s) => String(s ?? "").trim().toLowerCase();

  // Accept a few common ratio-row spellings just in case payload differs
  const isRatioRowKey = (rowKey) => {
    const k = norm(rowKey).replace(/\s+/g, "");

    // handle both display labels and backend row keys
    return (
      k === "s1/s2" ||
      k === "s3/s4" ||
      k === "s1:s2" ||
      k === "s3:s4" ||
      k === "s1_s2" ||
      k === "s3_s4"
    );
  };

  const looksNumeric = (s) => {
    // Allows: 1, -1, 1.23, .5, 1e-3, -2.4E+2
    return /^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$/.test(s);
  };

  const formatValue = (v, rowKey) => {
    if (v === null || v === undefined) return "";

    // Strings can be:
    // - real text like "ALL PREY" -> preserve
    // - numeric strings like "0.96875" -> treat as number (esp for ratio rows)
    if (typeof v === "string") {
      const s = v.trim();
      if (!s) return "";

      const sLower = s.toLowerCase();
      if (sLower === "nan" || sLower === "none" || sLower === "null" || sLower === "undefined") return "";

      if (looksNumeric(s)) {
        const numFromString = Number(s);
        if (Number.isFinite(numFromString)) {
          v = numFromString; // fall through to numeric formatting
        } else {
          return "";
        }
      } else {
        // Non-numeric string like "ALL PREY" -> preserve
        return s;
      }
    }

    const num = typeof v === "number" ? v : Number(v);
    if (!Number.isFinite(num)) return "";

    // ✅ Always format ratio rows to 3 decimals
    if (isRatioRowKey(rowKey)) {
      return num.toFixed(3);
    }

    // Non-ratio: keep natural display
    if (Math.abs(num - Math.round(num)) < 1e-12) return String(Math.round(num));
    return String(Number(num.toPrecision(6)));
  };

  // Avoid duplicate headings when the set label already equals the variable label
  const setLabel = safeStr(setPayload.label, "");
  const rawVarLabel = variableLabel || variableKey;

  const prettyTitle =
    typeof dedupePercentTitle === "function"
      ? dedupePercentTitle(setLabel, rawVarLabel)
      : rawVarLabel;

  // ----------------------------
  // Build DOM
  // ----------------------------
  const wrapper = document.createElement("div");
  wrapper.className = "mb-4";

  // Variable heading
  const h6 = document.createElement("h6");
  h6.className = "mt-3 mb-2 fw-bold";
  h6.textContent = prettyTitle;
  wrapper.appendChild(h6);

  // ✅ Variable note directly under heading
  const noteText = safeStr(variableNote, "");
  if (noteText) {
    const p = document.createElement("p");
    p.className = "text-muted mb-2";
    p.textContent = noteText;
    wrapper.appendChild(p);
  }

  const tableResponsive = document.createElement("div");
  tableResponsive.className = "table-responsive";

  const table = document.createElement("table");
  table.className = "table table-sm table-striped table-bordered";

  // Header
  const thead = document.createElement("thead");
  const trh = document.createElement("tr");

  const th0 = document.createElement("th");
  th0.textContent = "Location";
  trh.appendChild(th0);

  for (const p of periods) {
    const th = document.createElement("th");
    th.textContent = safeStr(p.label, safeStr(p.key, ""));
    trh.appendChild(th);
  }
  thead.appendChild(trh);
  table.appendChild(thead);

  // Body
  const tbody = document.createElement("tbody");

  for (const rowKey of rows) {
    const tr = document.createElement("tr");

    const td0 = document.createElement("td");
    td0.textContent = safeStr(rowLabels[rowKey], rowKey);
    tr.appendChild(td0);

    const rowMap = varBlock[rowKey];

    for (const p of periods) {
      const pk = safeStr(p.key, "");
      const td = document.createElement("td");

      let v = null;
      if (isObject(rowMap) && pk) v = rowMap[pk];

      td.textContent = formatValue(v, rowKey);
      tr.appendChild(td);
    }

    tbody.appendChild(tr);
  }

  table.appendChild(tbody);
  tableResponsive.appendChild(table);
  wrapper.appendChild(tableResponsive);

  return wrapper;
}