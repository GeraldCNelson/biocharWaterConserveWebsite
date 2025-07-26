// static/js/custom_gseason.js

// export two functions: one to initialize your data model, one to render it

let _DEFAULT_YEAR;
let _YEARS;
let _DEFAULT_PERIODS;
let _periodsData = [];

/**
 * Call this once, after you've injected your HTML into #gseason-content.
 *
 * @param {number} DEFAULT_YEAR
 * @param {number[]} YEARS
 * @param {{code:string,label:string,start:string,end:string}[]} DEFAULT_PERIODS
 */
export function initCustomGseason(DEFAULT_YEAR, YEARS, DEFAULT_PERIODS) {
  _DEFAULT_YEAR    = DEFAULT_YEAR;
  _YEARS           = YEARS;
  _DEFAULT_PERIODS = DEFAULT_PERIODS;

  // populate the anchor‐year dropdown
  const yearSelect = document.getElementById("anchor-year");
  yearSelect.innerHTML = "";
  _YEARS.forEach((y) => {
    const o = document.createElement("option");
    o.value = y;
    o.textContent = y;
    if (y === _DEFAULT_YEAR) o.selected = true;
    yearSelect.appendChild(o);
  });

  // wire up the “Add Period” button
  document.getElementById("add-period").onclick = () => {
    const anchor = parseInt(yearSelect.value, 10);
    const newIdx = _periodsData.length + 1;
    _periodsData.push({
      code:      `CUSTOM_${newIdx}`,
      isDefault: false,
      label:     "",
      start:     `${anchor}-01-01`,
      end:       `${anchor}-12-31`,
    });
    renderCustomGseason();
  };

  // whenever year changes, re-render
  yearSelect.onchange = renderCustomGseason;

  // seed your in-memory array from the defaults
  resetPeriodsData();
  renderCustomGseason();
}

/** Clear and repopulate `_periodsData` from the defaults */
function resetPeriodsData() {
  const anchor = _DEFAULT_YEAR;
  _periodsData = _DEFAULT_PERIODS.map((p) => {
    const [sm] = p.start.split("-");
    const [em] = p.end.split("-");
    const endYear = parseInt(em, 10) < parseInt(sm, 10) ? anchor + 1 : anchor;
    return {
      code:      p.code,
      isDefault: true,
      label:     p.label,
      startMD:   p.start,
      endMD:     p.end,
      start:     `${anchor}-${p.start}`,
      end:       `${endYear}-${p.end}`,
    };
  });
}

/** Build the rows in #periods-container */
export function renderCustomGseason() {
  const container  = document.getElementById("periods-container");
  const yearSelect = document.getElementById("anchor-year");
  const anchor     = parseInt(yearSelect.value, 10);

  container.innerHTML = ""; // clear

  _periodsData.forEach((p, idx) => {
    // recompute if it’s a default period
    if (p.isDefault) {
      const [sm] = p.startMD.split("-");
      const [em] = p.endMD.split("-");
      const endYear = parseInt(em, 10) < parseInt(sm, 10) ? anchor + 1 : anchor;
      p.start = `${anchor}-${p.startMD}`;
      p.end   = `${endYear}-${p.endMD}`;
    }

    const row = document.createElement("div");
    row.className = "mb-3 p-3 border rounded bg-light";
    row.dataset.index = idx;
    row.innerHTML = /* html */`
      <button type="button" class="btn-close float-end remove-period"
              aria-label="Remove period"></button>

      <div class="mb-2">
        <label class="form-label">Period ${idx+1} Name</label>
        <input type="text" class="form-control period-label"
               value="${p.label}" placeholder="Enter name" />
      </div>

      <div class="row g-2">
        <div class="col">
          <label class="form-label">Start</label>
          <input type="date" class="form-control period-start"
                 value="${p.start}" />
        </div>
        <div class="col">
          <label class="form-label">End</label>
          <input type="date" class="form-control period-end"
                 value="${p.end}" />
        </div>
      </div>
    `;
    container.appendChild(row);

    // handlers
    row.querySelector(".remove-period").onclick = () => {
      _periodsData.splice(idx, 1);
      renderCustomGseason();
    };
    row.querySelector(".period-label").oninput  = e => { _periodsData[idx].label = e.target.value; };
    row.querySelector(".period-start").onchange = e => { _periodsData[idx].start = e.target.value; };
    row.querySelector(".period-end").onchange   = e => { _periodsData[idx].end   = e.target.value; };
  });
}