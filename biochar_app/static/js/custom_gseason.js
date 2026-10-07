// @ts-check
// static/js/custom_gseason.js

/**
 * Initialize the “Custom Season” editor.
 *
 * @param {{
 *   defaultYear: number,
 *   years: number[],
 *   defaultPeriods: Array<{code:string,label:string,start:string,end:string}>,
 *   monthAbbr?: Record<string, string>
 * }} cfg
 */
export function nextCustomPeriodCode(periods) {
  const used = new Set(periods.map((period) => String(period?.code || "")));
  let candidate = 1;
  while (used.has(`CUSTOM_${candidate}`)) candidate += 1;
  return `CUSTOM_${candidate}`;
}

export function initCustomGseason(cfg) {
  const {
    defaultYear,
    years,
    defaultPeriods,
    monthAbbr = {},
  } = cfg;

  /** @type {Array<{
   *   code: string,
   *   isDefault: boolean,
   *   label: string,
   *   start: string,
   *   end: string,
   *   startMD?: string,
   *   endMD?: string
   * }>} */
  let periodsData = [];
  const seasonWindow = /** @type {any} */ (window);
  const applyBtn = document.getElementById("apply-seasons");
  const applyStatus = document.getElementById("apply-seasons-status");
  function markPending() {
    if (applyStatus) applyStatus.textContent = "Unapplied changes. Click Apply Seasons when finished.";
  }
  function applySeasons() {
    // Read committed native-control values even if a change event is pending.
    const rows = Array.from(containerEl.children);
    const snapshot = periodsData.map((p, idx) => ({
      code: p.code, label: rows[idx]?.querySelector(".period-label")?.value ?? p.label,
      start: rows[idx]?.querySelector(".period-start")?.value || "",
      end: rows[idx]?.querySelector(".period-end")?.value || "",
    }));
    const realDate = (s) => /^\d{4}-\d{2}-\d{2}$/.test(s) &&
      Number.isFinite(Date.parse(s)) && new Date(s).toISOString().slice(0, 10) === s;
    if (!snapshot.length || snapshot.some(p => !p.label.trim() || !realDate(p.start) || !realDate(p.end) || p.start > p.end)) {
      if (applyStatus) applyStatus.textContent = "Enter a name and valid start/end dates for every period; the start must not follow the end.";
      return;
    }
    seasonWindow.appliedCustomSeasons = { anchorYear: Number(yearSelectEl.value), periods: snapshot };
    if (applyStatus) applyStatus.textContent = "Seasons applied. Select Seasonal Periods, then update your plots or summary.";
  }

  const yearSelect = /** @type {HTMLSelectElement | null} */ (
    document.getElementById("anchor-year")
  );
  const addPeriodBtn = /** @type {HTMLButtonElement | null} */ (
    document.getElementById("add-period")
  );
  const container = /** @type {HTMLDivElement | null} */ (
    document.getElementById("periods-container")
  );

  if (!yearSelect || !addPeriodBtn || !container) {
    console.warn("⚠️ Custom gseason controls not found.");
    return () => periodsData;
  }

  const yearSelectEl = yearSelect;
  const addPeriodBtnEl = addPeriodBtn;
  const containerEl = container;

  function formatDateLabel(dateString) {
    if (!dateString) return "";

    const parts = dateString.split("-");
    if (parts.length !== 3) return dateString;

    const month = monthAbbr?.[parts[1]] || parts[1];
    const day = parseInt(parts[2], 10);

    return `${month} ${day}`;
  }

  function updateSeasonalPeriodSummary() {
    const summaryEl = document.getElementById("seasonal-period-summary");
    if (!summaryEl) return;

    if (!periodsData.length) {
      summaryEl.textContent = "";
      return;
    }

    const labels = periodsData
      .filter((p) => p.label && p.start && p.end)
      .map((p) => `${p.label}: ${formatDateLabel(p.start)}–${formatDateLabel(p.end)}`);

    summaryEl.innerHTML = `
      <strong>Current seasonal periods:</strong>
      ${labels.join("; ")}
    `;
  }

  // 1) populate anchor-year dropdown
  yearSelectEl.innerHTML = "";
  years.forEach((y) => {
    const opt = document.createElement("option");
    opt.value = String(y);
    opt.textContent = String(y);
    if (y === defaultYear) opt.selected = true;
    yearSelectEl.appendChild(opt);
  });

  // 2) initialize periodsData from defaults
  function initPeriodsData() {
    periodsData = defaultPeriods.map((p) => {
      const startMD = p.start.slice(-5);
      const endMD = p.end.slice(-5);
      const [sm] = startMD.split("-");
      const [em] = endMD.split("-");
      const wraps = parseInt(sm, 10) > parseInt(em, 10);
      const startYear = wraps ? defaultYear - 1 : defaultYear;
      const endYear = wraps ? defaultYear : defaultYear;

      return {
        code: p.code,
        isDefault: true,
        label: p.label,
        startMD,
        endMD,
        start: `${startYear}-${startMD}`,
        end: `${endYear}-${endMD}`,
      };
    });
  }

  // 3) render all period rows
  function renderPeriods() {
    containerEl.innerHTML = "";
    const anchor = parseInt(yearSelectEl.value, 10);

    periodsData.forEach((p, idx) => {
      if (p.isDefault && p.startMD && p.endMD) {
        const [sm] = p.startMD.split("-");
        const [em] = p.endMD.split("-");
        const wraps = parseInt(sm, 10) > parseInt(em, 10);
        const startYear = wraps ? anchor - 1 : anchor;
        const endYear = wraps ? anchor : anchor;
        p.start = `${startYear}-${p.startMD}`;
        p.end = `${endYear}-${p.endMD}`;
      }

      const row = document.createElement("div");
      row.className = "mb-3 p-3 border rounded bg-light";
      row.classList.add("period-row");
      row.dataset.index = String(idx);
      row.dataset.code = p.code;

      row.innerHTML = `
        <button
          type="button"
          class="btn-close float-end remove-period"
          aria-label="Remove period"
        ></button>
        <div class="mb-2">
          <label class="form-label">Period ${idx + 1} Name</label>
          <input
            type="text"
            class="form-control period-label"
            value="${p.label}"
            placeholder="Enter name"
          />
        </div>
        <div class="row g-2">
          <div class="col">
            <label class="form-label">Start</label>
            <input
              type="date"
              class="form-control period-start"
              value="${p.start}"
            />
          </div>
          <div class="col">
            <label class="form-label">End</label>
            <input
              type="date"
              class="form-control period-end"
              value="${p.end}"
            />
          </div>
        </div>
      `;
      containerEl.appendChild(row);

      const removeBtn = /** @type {HTMLButtonElement | null} */ (
        row.querySelector(".remove-period")
      );
      const labelInput = /** @type {HTMLInputElement | null} */ (
        row.querySelector(".period-label")
      );
      const startInput = /** @type {HTMLInputElement | null} */ (
        row.querySelector(".period-start")
      );
      const endInput = /** @type {HTMLInputElement | null} */ (
        row.querySelector(".period-end")
      );

      if (removeBtn) {
        removeBtn.onclick = () => {
          periodsData.splice(idx, 1);
          renderPeriods();
          markPending();
        };
      }

      if (labelInput) {
        labelInput.value = p.label;
        labelInput.oninput = (e) => {
          const target = /** @type {HTMLInputElement | null} */ (e.target);
          periodsData[idx].label = target?.value || "";
          updateSeasonalPeriodSummary();
          markPending();
        };
      }

      if (startInput) {
        // Set native date-control state explicitly after inserting the row.
        // Ignore events from controls detached by an anchor-year re-render.
        startInput.value = p.start;
        startInput.onchange = (e) => {
          const target = /** @type {HTMLInputElement | null} */ (e.target);
          if (!target?.isConnected) return;
          p.start = target.value;
          p.isDefault = false;
          updateSeasonalPeriodSummary();
          markPending();
        };
      }

      if (endInput) {
        endInput.value = p.end;
        endInput.onchange = (e) => {
          const target = /** @type {HTMLInputElement | null} */ (e.target);
          if (!target?.isConnected) return;
          p.end = target.value;
          p.isDefault = false;
          updateSeasonalPeriodSummary();
          markPending();
        };
      }
          });

    updateSeasonalPeriodSummary();
  }

  // 4) “+ Add Period” button
  addPeriodBtnEl.onclick = () => {
    const anchor = parseInt(yearSelectEl.value, 10);
    const code = nextCustomPeriodCode(periodsData);

    periodsData.push({
      code,
      isDefault: false,
      label: "",
      start: `${anchor}-01-01`,
      end: `${anchor}-12-31`,
    });

    renderPeriods();
    markPending();
  };

  // 5) re-render on anchor-year change
  let displayedAnchor = Number(yearSelectEl.value);
  yearSelectEl.onchange = () => {
    const newAnchor = Number(yearSelectEl.value);
    const delta = newAnchor - displayedAnchor;
    for (const period of periodsData) {
      if (period.isDefault) continue;
      for (const bound of ["start", "end"]) {
        const value = period[bound];
        if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) continue;
        const [year, month, day] = value.split("-").map(Number);
        const shiftedYear = year + delta;
        const shiftedDay = Math.min(day, new Date(shiftedYear, month, 0).getDate());
        period[bound] = `${shiftedYear}-${String(month).padStart(2, "0")}-${String(shiftedDay).padStart(2, "0")}`;
      }
    }
    displayedAnchor = newAnchor;
    renderPeriods(); markPending();
  };
  if (applyBtn) applyBtn.onclick = applySeasons;

  // 6) initial bootstrap
  initPeriodsData();
  renderPeriods();
  applySeasons();

  // 7) expose periodsData for debugging just before you POST
  return () => periodsData;
}
