// @ts-check

const STORAGE_KEY = "biochar_active_irrigation_events";
const FORM_STORAGE_KEY = "biochar_irrigation_entry_form";

/**
 * @param {string} id
 * @returns {HTMLElement}
 */
function mustGet(id) {
  const el = document.getElementById(id);
  if (!el) throw new Error(`Missing element #${id}`);
  return el;
}

/**
 * @param {string} id
 * @returns {HTMLInputElement}
 */
function input(id) {
  return /** @type {HTMLInputElement} */ (mustGet(id));
}

/**
 * @param {string} id
 * @returns {HTMLTextAreaElement}
 */
function textarea(id) {
  return /** @type {HTMLTextAreaElement} */ (mustGet(id));
}

/**
 * @param {string} id
 * @returns {HTMLSelectElement}
 */
function select(id) {
  return /** @type {HTMLSelectElement} */ (mustGet(id));
}

/**
 * @param {number} n
 * @returns {string}
 */
function pad2(n) {
  return String(n).padStart(2, "0");
}

/**
 * @param {string} id
 * @param {string} value
 * @returns {void}
 */
function setInputValue(id, value) {
  const el = input(id);
  el.value = value || "";
  el.dataset.savedValue = value || "";
}

/**
 * @param {string} id
 * @returns {string}
 */
function getInputValue(id) {
  const el = input(id);
  return (el.value || el.dataset.savedValue || "").trim();
}

/**
 * @param {"start" | "end"} prefix
 * @returns {void}
 */
function fillPhoneTime(prefix) {
  const d = new Date();
  let hour = d.getHours();
  const minute = d.getMinutes();
  const ampm = hour >= 12 ? "PM" : "AM";

  hour = hour % 12;
  if (hour === 0) hour = 12;

  setInputValue(
    `${prefix}-date`,
    `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`
  );
  setInputValue(`${prefix}-time`, `${hour}:${pad2(minute)}`);
  select(`${prefix}-ampm`).value = ampm;

  saveFormState();
}

/**
 * @param {string} raw
 * @returns {string | null}
 */
function normalizeDateValue(raw) {
  const s = String(raw || "").trim();
  if (!s) return null;

  let match = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (match) return s;

  match = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2}|\d{4})$/);
  if (match) {
    const month = Number(match[1]);
    const day = Number(match[2]);
    let year = Number(match[3]);

    if (year < 100) year += 2000;

    if (
      !Number.isFinite(year) ||
      !Number.isFinite(month) ||
      !Number.isFinite(day) ||
      month < 1 ||
      month > 12 ||
      day < 1 ||
      day > 31
    ) {
      return null;
    }

    return `${year}-${pad2(month)}-${pad2(day)}`;
  }

  return null;
}

/**
 * @param {"start" | "end"} prefix
 * @returns {string | null}
 */
function getTimestampFromParts(prefix) {
  const rawDateValue = getInputValue(`${prefix}-date`);
  const dateValue = normalizeDateValue(rawDateValue);
  const timeValue = getInputValue(`${prefix}-time`);
  const ampm = select(`${prefix}-ampm`).value;

  if (!dateValue || !timeValue) return null;

  const match = timeValue.match(/^(\d{1,2})(?::(\d{2}))?$/);
  if (!match) return null;

  let hour = Number(match[1]);
  const minute = Number(match[2] || "0");

  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return null;
  if (hour < 1 || hour > 12 || minute < 0 || minute > 59) return null;

  if (ampm === "PM" && hour !== 12) hour += 12;
  if (ampm === "AM" && hour === 12) hour = 0;

  return `${dateValue}T${pad2(hour)}:${pad2(minute)}`;
}

/**
 * @param {string} msg
 * @param {"info" | "success" | "warning" | "danger"} [kind]
 */
function setStatus(msg, kind = "info") {
  const box = mustGet("status-box");
  box.className = `alert alert-${kind} border status-box`;
  box.textContent = msg;
}

/**
 * @returns {Array<{strip_group: string, location: string}>}
 */
function selectedGroups() {
  const checks = Array.from(document.querySelectorAll(".strip-group-check"));
  return checks
    .filter((el) => /** @type {HTMLInputElement} */ (el).checked)
    .map((el) => {
      const inp = /** @type {HTMLInputElement} */ (el);
      return {
        strip_group: inp.value,
        location: inp.dataset.location || "",
      };
    });
}

/**
 * @returns {number}
 */
function selectedFlowAllocationFraction() {
  const flowSelect = select("flow-allocation");
  const value = Number(flowSelect.value || "1");
  return Number.isFinite(value) ? value : 1;
}

/**
 * @param {string} value
 * @returns {number | null}
 */
function nullableNumber(value) {
  const s = String(value || "").trim();
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

/**
 * @returns {Record<string, string>}
 */
function loadActiveEvents() {
  try {
    return JSON.parse(localStorage.getItem(STORAGE_KEY) || "{}");
  } catch {
    return {};
  }
}

/**
 * @param {Record<string, string>} events
 */
function saveActiveEvents(events) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(events));
}

/**
 * @returns {void}
 */
function saveFormState() {
  const state = {
    enteredBy: input("entered-by").value,
    groupWest: input("group-west").checked,
    groupEast: input("group-east").checked,
    flowAllocation: select("flow-allocation").value,

    startDate: input("start-date").value,
    startTime: input("start-time").value,
    startAmpm: select("start-ampm").value,
    startTotalizer: input("start-totalizer").value,
    startFlow: input("start-flow").value,
    startNotes: textarea("start-notes").value,

    endDate: input("end-date").value,
    endTime: input("end-time").value,
    endAmpm: select("end-ampm").value,
    endTotalizer: input("end-totalizer").value,
    endFlow: input("end-flow").value,
    endNotes: textarea("end-notes").value,
  };

  localStorage.setItem(FORM_STORAGE_KEY, JSON.stringify(state));
}

/**
 * @returns {void}
 */
function restoreFormState() {
  try {
    const raw = localStorage.getItem(FORM_STORAGE_KEY);
    if (!raw) return;

    const state = JSON.parse(raw);

    setInputValue("entered-by", state.enteredBy || "");

    input("group-west").checked = Boolean(state.groupWest);
    input("group-east").checked = Boolean(state.groupEast);

    setInputValue("start-date", state.startDate || "");
    setInputValue("start-time", state.startTime || "");
    select("start-ampm").value = state.startAmpm || "AM";
    setInputValue("start-totalizer", state.startTotalizer || "");
    setInputValue("start-flow", state.startFlow || "");
    textarea("start-notes").value = state.startNotes || "";

    setInputValue("end-date", state.endDate || "");
    setInputValue("end-time", state.endTime || "");
    select("end-ampm").value = state.endAmpm || "AM";
    setInputValue("end-totalizer", state.endTotalizer || "");
    setInputValue("end-flow", state.endFlow || "");
    textarea("end-notes").value = state.endNotes || "";

    if (state.flowAllocation) {
      select("flow-allocation").value = state.flowAllocation;
    }
  } catch (err) {
    console.warn("Unable to restore irrigation form state:", err);
  }
}

/**
 * @param {string} endpoint
 * @param {any} payload
 * @returns {Promise<any>}
 */
async function postJson(endpoint, payload) {
  const resp = await fetch(endpoint, {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify(payload),
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(text || `HTTP ${resp.status}`);
  }

  return await resp.json();
}

async function refreshRecentEvents() {
  const container = mustGet("recent-events");

  try {
    const resp = await fetch("/api/management/irrigation/events?limit=10");
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);

    const data = await resp.json();
    const events = Array.isArray(data.events) ? data.events : [];

    if (!events.length) {
      container.textContent = "No irrigation events yet.";
      return;
    }

    container.innerHTML = events.map((ev) => {
      const totalGallons = ev.gallons == null
        ? ""
        : ` — total meter: ${Number(ev.gallons).toLocaleString()} gal`;

      const allocatedGallons = ev.allocated_gallons == null
        ? ""
        : ` — allocated: ${Number(ev.allocated_gallons).toLocaleString()} gal`;

      const startPhoto = ev.start_photo
        ? ` — <a href="${ev.start_photo}" target="_blank">start photo</a>`
        : "";

      const endPhoto = ev.end_photo
        ? ` — <a href="${ev.end_photo}" target="_blank">end photo</a>`
        : "";

      return `
        <div class="border rounded p-2 mb-2 bg-white">
          <div><strong>${ev.date || ""}</strong> ${ev.strip_group || ""} (${ev.location || ""})</div>
          <div>Status: ${ev.status || ""}${totalGallons}${allocatedGallons}${startPhoto}${endPhoto}</div>
          <div class="text-muted">${ev.event_id || ""}</div>
        </div>
      `;
    }).join("");
  } catch (err) {
    console.error(err);
    container.textContent = "Unable to load recent events.";
  }
}

function updateFlowAllocationUI() {
  const flowSelect = /** @type {HTMLSelectElement | null} */ (
    document.getElementById("flow-allocation")
  );

  if (!flowSelect) return;

  const selected = selectedGroups();

  if (selected.length === 0) {
    flowSelect.value = "1";
    flowSelect.disabled = true;
    return;
  }

  if (selected.length === 1) {
    flowSelect.value = "1";
    flowSelect.disabled = true;
    return;
  }

  flowSelect.disabled = false;
  flowSelect.value = "0.5";
}

/**
 * @param {string} eventId
 * @param {"start" | "end"} photoType
 * @param {HTMLInputElement} fileInput
 * @returns {Promise<string | null>}
 */
async function uploadPhotoIfPresent(eventId, photoType, fileInput) {
  const file = fileInput.files && fileInput.files[0];
  if (!file) return null;

  const formData = new FormData();
  formData.append("file", file);

  const resp = await fetch(`/api/management/irrigation/${eventId}/photo/${photoType}`, {
    method: "POST",
    body: formData,
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(text || `Photo upload failed: HTTP ${resp.status}`);
  }

  const data = await resp.json();
  return data.photo_path || data.path || null;
}

async function startIrrigation() {
  updateFlowAllocationUI();

  const groups = selectedGroups();
  if (!groups.length) {
    setStatus("Please select at least one strip group.", "warning");
    return;
  }

  const flowFraction = selectedFlowAllocationFraction();

  const startTimestampInput = getTimestampFromParts("start");
  if (!startTimestampInput) {
    setStatus("Please enter a valid start date and time.", "warning");
    return;
  }

  const enteredBy = input("entered-by").value.trim();
  if (!enteredBy) {
    setStatus("Please enter initials/name abbreviation.", "warning");
    return;
  }

  const payloadBase = {
    start_timestamp: startTimestampInput,
    start_totalizer_gal_x100: nullableNumber(input("start-totalizer").value),
    start_flow_gpm: nullableNumber(input("start-flow").value),
    flow_allocation_fraction: flowFraction,
    entered_by: enteredBy,
    notes: textarea("start-notes").value.trim(),
  };

  const active = loadActiveEvents();
  const created = [];
  const createdEventIds = [];

  try {
    for (const group of groups) {
      const payload = {
        ...payloadBase,
        strip_group: group.strip_group,
        location: group.location,
      };

      const data = await postJson("/api/management/irrigation/start", payload);
      active[group.strip_group] = data.event_id;
      created.push(`${group.strip_group}: ${data.event_id}`);
      createdEventIds.push(data.event_id);
    }

    let photoMsg = "";
    if (createdEventIds.length) {
      const photoPath = await uploadPhotoIfPresent(
        createdEventIds[0],
        "start",
        input("start-photo")
      );
      if (photoPath) {
        photoMsg = `\nStart photo uploaded: ${photoPath}`;
      }
    }

    saveActiveEvents(active);
    setStatus(`Started irrigation event(s):\n${created.join("\n")}${photoMsg}`, "success");
    await refreshRecentEvents();
  } catch (err) {
    console.error(err);
    setStatus(
      `Could not complete start irrigation.\nYour form entries are saved in this browser.\n\n${err}`,
      "warning"
    );
  }
}

async function finishIrrigation() {
  saveFormState();

  const groups = selectedGroups();
  if (!groups.length) {
    setStatus("Please select the strip group(s) to finish.", "warning");
    return;
  }

  const endTimestampInput = getTimestampFromParts("end");
  if (!endTimestampInput) {
    setStatus("Please enter a valid end date and time.", "warning");
    return;
  }

  const active = loadActiveEvents();
  const finished = [];
  const finishedEventIds = [];

  const payload = {
    end_timestamp: endTimestampInput,
    end_totalizer_gal_x100: nullableNumber(input("end-totalizer").value),
    end_flow_gpm: nullableNumber(input("end-flow").value),
    notes: textarea("end-notes").value.trim(),
  };

  try {
    for (const group of groups) {
      const eventId = active[group.strip_group];
      if (!eventId) {
        throw new Error(`No active event found for ${group.strip_group}.`);
      }

      const data = await postJson(`/api/management/irrigation/${eventId}/finish`, payload);
      finished.push(`${group.strip_group}: ${data.event_id}`);
      finishedEventIds.push(data.event_id);
      delete active[group.strip_group];
    }

    let photoMsg = "";
    if (finishedEventIds.length) {
      const photoPath = await uploadPhotoIfPresent(
        finishedEventIds[0],
        "end",
        input("end-photo")
      );
      if (photoPath) {
        photoMsg = `\nEnd photo uploaded: ${photoPath}`;
      }
    }

    saveActiveEvents(active);
    setStatus(`Finished irrigation event(s):\n${finished.join("\n")}${photoMsg}`, "success");
    await refreshRecentEvents();
  } catch (err) {
    console.error(err);
    setStatus(
      `Failed to finish irrigation event.\nYour form entries are saved in this browser.\n\n${err}`,
      "warning"
    );
  }
}

async function exportIrrigationCleanCsv() {
  try {
    const resp = await fetch("/api/management/irrigation/export-and-rebuild", {
      method: "POST",
    });

    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(text || `HTTP ${resp.status}`);
    }

    const data = await resp.json();

    const exportRows = data.export?.rows ?? 0;
    const rebuildRows = data.rebuild?.rows ?? 0;
    const outputCsv = data.rebuild?.output_csv || "";

    setStatus(
      `Export and rebuild complete.\n` +
      `Rows exported from field entries: ${exportRows}\n` +
      `Rows in irrigation_clean.csv: ${rebuildRows}\n` +
      `Output: ${outputCsv}`,
      "success"
    );

    await refreshRecentEvents();
  } catch (err) {
    console.error(err);
    setStatus(`Export/rebuild failed:\n${err}`, "danger");
  }
}

function attachAutosave() {
  const ids = [
    "entered-by",
    "group-west",
    "group-east",
    "flow-allocation",
    "start-date",
    "start-time",
    "start-ampm",
    "start-totalizer",
    "start-flow",
    "start-notes",
    "end-date",
    "end-time",
    "end-ampm",
    "end-totalizer",
    "end-flow",
    "end-notes",
  ];

  ids.forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;

    el.addEventListener("input", () => {
      updateFlowAllocationUI();
      saveFormState();
    });

    el.addEventListener("change", () => {
      updateFlowAllocationUI();
      saveFormState();
    });
  });
}

function init() {
  restoreFormState();
  updateFlowAllocationUI();
  attachAutosave();

  mustGet("capture-start-time").addEventListener("click", () => {
    fillPhoneTime("start");
  });

  mustGet("capture-end-time").addEventListener("click", () => {
    fillPhoneTime("end");
  });

  mustGet("start-irrigation").addEventListener("click", () => {
    void startIrrigation();
  });

  mustGet("finish-irrigation").addEventListener("click", () => {
    void finishIrrigation();
  });

  const exportBtn = document.getElementById("export-irrigation-clean-csv");
  if (exportBtn) {
    exportBtn.addEventListener("click", () => {
      void exportIrrigationCleanCsv();
    });
  }

  void refreshRecentEvents();
}

document.addEventListener("DOMContentLoaded", init);