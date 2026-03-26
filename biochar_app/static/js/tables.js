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
// Note rendering helpers (safe URL -> clickable link)
// ------------------------------------------------------------

/**
 * Append a note string to a container, converting plain http(s) URLs into
 * clickable <a> links WITHOUT using innerHTML (avoids XSS).
 *
 * Example: "Source: https://example.com/foo" => Source: [link]
 */
function appendTextWithLinks(parentEl, text) {
  const raw = safeStr(text, "");
  if (!raw) return;

  // Basic URL matcher: stop at whitespace or common closing punctuation.
  // (We intentionally keep it conservative.)
  const urlRe = /(https?:\/\/[^\s<>"'()]+)(?=[\s<>"'()]|$)/g;

  let last = 0;
  let m;

  while ((m = urlRe.exec(raw)) !== null) {
    const start = m.index;
    const url = m[1];

    // preceding text
    if (start > last) {
      parentEl.appendChild(document.createTextNode(raw.slice(last, start)));
    }

    // link
    const a = document.createElement("a");
    a.href = url;
    a.textContent = url;
    a.target = "_blank";
    a.rel = "noopener noreferrer";
    parentEl.appendChild(a);

    last = start + url.length;
  }

  // trailing text
  if (last < raw.length) {
    parentEl.appendChild(document.createTextNode(raw.slice(last)));
  }
}

// ------------------------------------------------------------
// Normalization
// ------------------------------------------------------------

function normalizeOneSet(s, idx = 0, totalSets = null) {
  const set = isObject(s) ? s : {};

  // Pull raw label from backend (do NOT mutate it yet)
  const rawLabel =
    safeStr(set.label, "") ||
    safeStr(set.title, "") ||
    "";

  const isAlreadyNumbered = /^\s*set\s*\d+\s*:/i.test(rawLabel);

  // Decide whether to auto-number:
  // - If backend already numbered → keep it
  // - If multiple sets exist → add "Set X:"
  // - If only one set → leave unnumbered
  const shouldNumber =
    !isAlreadyNumbered &&
    (totalSets === null || totalSets > 1);

  const label = rawLabel
    ? (
        isAlreadyNumbered
          ? rawLabel
          : (shouldNumber ? `Set ${idx + 1}: ${rawLabel}` : rawLabel)
      )
    : `Set ${idx + 1}`;

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
      const k =
        safeStr(x.key, "") ||
        safeStr(x.id, "") ||
        safeStr(x.name, "");

      const lbl =
        safeStr(x.label, "") ||
        safeStr(x.title, "") ||
        k;

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
  const note = safeStr(set.note, "") || safeStr(set["notes"], "");

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
 *  C) Legacy wrapper:     { title, set: { ...single set... } }
 */
export function normalizePayload(raw) {
  if (!isObject(raw)) return { title: "", sets: [] };

  const title = safeStr(raw.title, "") || safeStr(raw.label, "");

  // Multi-set (preferred)
  if (Array.isArray(raw.sets)) {
    return {
      title,
      sets: raw.sets.map((s, i) => normalizeOneSet(s, i, raw.sets.length)),
    };
  }

  // Legacy wrapper: { set: { ... } }
  if (isObject(raw.set)) {
    const set = normalizeOneSet(
      {
        ...raw.set,
        // If a legacy wrapper doesn't provide a set label, fall back to the payload title
        label: safeStr(raw.set.label, "") || safeStr(raw.set.title, "") || title || "Set 1",
      },
      0,
      1
    );
    return { title, sets: [set] };
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
        label: title || "Set 1",
        periods: raw.periods,
        variables: raw.variables,
        rows: raw.rows,
        rowLabels: raw.rowLabels,
        data: raw.data,
        note: safeStr(raw.note, "") || safeStr(raw["notes"], ""),
      },
      0,
      1
    );

    return { title, sets: [set] };
  }

  // Unknown shape
  return { title, sets: [] };
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
      {
        label: setPayload.label,
        periods,
        rows,
        rowLabels,
        data,
        variables, // keep full normalized variable metadata available
      },
      varKey,
      varLabel,
      safeStr(v.note, "")
    );

    parentEl.appendChild(tableEl);
  }
}

// Small helper for safe inline JSON display in HTML warnings.

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

/**
 * Build a single variable table.
 * Expects:
 *   periods: [{key,label}, ...]
 *   rows: [rowKey, ...]
 *   rowLabels: {rowKey: label}
 *   data: { varKey: { rowKey: { periodKey: value|null } } }
 */

function openReferenceModal(variable) {
  if (!variable || !variable["reference"]) return;

  const modalTitle = document.getElementById("referenceModalLabel");
  const modalBody = document.getElementById("referenceModalBody");
  const modalEl = document.getElementById("referenceModal");

  if (!modalTitle || !modalBody || !modalEl) {
    console.warn("Reference modal elements not found.");
    return;
  }

  modalTitle.textContent = variable["label"] || "Reference";

  const ref = variable["reference"];
  const refs = Array.isArray(ref["references"]) ? ref["references"] : [];

  let html = "";

  if (ref["short_note"]) {
    html += `<p><strong>Summary:</strong> ${escapeHtml(ref["short_note"])}</p>`;
  }

  if (ref["detail"]) {
    html += `<p><strong>Detail:</strong> ${escapeHtml(ref["detail"])}</p>`;
  }

  if (ref["interpretation"]) {
    html += `<p><strong>Interpretation:</strong> ${escapeHtml(ref["interpretation"])}</p>`;
  }

  if (ref["caveat"]) {
    html += `<p><strong>Caution:</strong> ${escapeHtml(ref["caveat"])}</p>`;
  }

  if (refs.length > 0) {
    html += `<hr><h6>References</h6><ul>`;
    for (const r of refs) {
      const parts = [
        r["guide_label"],
        r["section_title"],
        r["table_number"],
        r["table_title"],
      ].filter(Boolean);

      const text = parts.join(" — ");
      if (r["source_url"]) {
        html += `<li><a href="${r["source_url"]}" target="_blank" rel="noopener noreferrer">${escapeHtml(text)}</a></li>`;
      } else {
        html += `<li>${escapeHtml(text)}</li>`;
      }
    }
    html += `</ul>`;
  }

  modalBody.innerHTML = html || "<p>No reference details available.</p>";

  if (!window.bootstrap || !window.bootstrap.Modal) {
    console.warn("Bootstrap Modal is not available.");
    return;
  }

  const modal = new window.bootstrap.Modal(modalEl);
  modal.show();
}

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

  // Look up the full variable metadata from the normalized set payload
  const variable =
    Array.isArray(setPayload.variables)
      ? setPayload.variables.find((v) => safeStr(v.key, "") === safeStr(variableKey, ""))
      : null;

  console.log("TABLE VARIABLE DEBUG:", variableKey, variable);
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

    // Always format ratio rows to 3 decimals
    if (isRatioRowKey(rowKey)) {
      return num.toFixed(3);
    }

    // Non-ratio: keep natural display
    if (Math.abs(num - Math.round(num)) < 1e-12) return String(Math.round(num));
    return String(Number(num.toPrecision(6)));
  };

  const prettyTitle = variableLabel || variableKey;

  // ----------------------------
  // Build DOM
  // ----------------------------
  const wrapper = document.createElement("div");
  wrapper.className = "mb-4";

  // Variable heading (with optional reference indicator)
  const h6 = document.createElement("h6");
  h6.className = "mt-3 mb-2 fw-bold d-flex align-items-center gap-2";

  const titleSpan = document.createElement("span");
  titleSpan.textContent = prettyTitle;
  h6.appendChild(titleSpan);

  const hasReference =
    variable?.["has_reference"] ||
    variable?.["reference_key"] ||
    variable?.["reference"];

  if (hasReference) {
    const refBadge = document.createElement("i");
    refBadge.className = "bi bi-info-circle info-icon";
    refBadge.setAttribute("role", "button");
    refBadge.setAttribute("tabindex", "0");
    refBadge.setAttribute("aria-label", `More information about ${prettyTitle}`);
    refBadge.style.cursor = "pointer";

    // Basic browser tooltip
    if (variable?.["reference"]?.["short_note"]) {
      refBadge.setAttribute("title", variable["reference"]["short_note"]);
    }

    // Click stub for future modal hookup
    refBadge.addEventListener("click", () => {
      openReferenceModal(variable);
    });

    h6.appendChild(refBadge);
  }

  wrapper.appendChild(h6);

  // Variable note directly under heading
  const noteText = safeStr(variableNote, "") || safeStr(variable?.note, "");
  if (noteText) {
    const p = document.createElement("p");
    p.className = "text-muted mb-2";
    appendTextWithLinks(p, noteText);
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