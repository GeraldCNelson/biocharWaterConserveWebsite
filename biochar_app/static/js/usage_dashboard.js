const metricDefinitions = [
  ["public_visits", "Public visits"],
  ["page_views", "Page views"],
  ["feature_uses", "Feature uses"],
  ["paper_downloads", "Paper downloads"],
  ["data_downloads", "Data downloads"],
  ["failed_downloads", "Failed downloads"],
];

const message = document.getElementById("usage-message");
const endDateInput = document.getElementById("usage-end-date");
const daysSelect = document.getElementById("usage-days");
const exportLink = document.getElementById("usage-export");

function textCell(value) {
  const cell = document.createElement("td");
  cell.textContent = value == null || value === "" ? "—" : String(value);
  return cell;
}

function emptyRow(columns, label = "No activity in this period") {
  const row = document.createElement("tr");
  row.className = "empty-row";
  const cell = textCell(label);
  cell.colSpan = columns;
  row.appendChild(cell);
  return row;
}

function renderMetrics(containerId, values) {
  const container = document.getElementById(containerId);
  container.replaceChildren();
  metricDefinitions.forEach(([key, label]) => {
    const card = document.createElement("div");
    card.className = "metric-card";
    const number = document.createElement("span");
    number.className = "metric-value";
    number.textContent = Number(values?.[key] || 0).toLocaleString();
    const caption = document.createElement("span");
    caption.className = "metric-label";
    caption.textContent = label;
    card.append(number, caption);
    container.appendChild(card);
  });
}

function renderComparison(previous, selected) {
  const body = document.getElementById("daily-comparison-body");
  body.replaceChildren();
  [selected, previous].forEach((item) => {
    const row = document.createElement("tr");
    row.append(
      textCell(item.date),
      textCell(item.public_visits),
      textCell(item.page_views),
      textCell(item.feature_uses),
      textCell(item.paper_downloads),
      textCell(item.data_downloads),
      textCell(item.failed_downloads),
      textCell(item.excluded_events),
    );
    body.appendChild(row);
  });
}

function renderTrend(rows) {
  const container = document.getElementById("usage-trend");
  container.replaceChildren();
  if (!rows.length) {
    const empty = document.createElement("p");
    empty.className = "trend-empty";
    empty.textContent = "No stored activity in this period.";
    container.appendChild(empty);
    return;
  }
  const maxValue = Math.max(1, ...rows.map((row) => Number(row.public_visits || 0)));
  rows.forEach((item) => {
    const row = document.createElement("div");
    row.className = "trend-row";
    const label = document.createElement("span");
    label.className = "trend-label";
    label.textContent = item.date;
    const track = document.createElement("div");
    track.className = "trend-track";
    const bar = document.createElement("div");
    bar.className = "trend-bar";
    bar.style.width = `${(Number(item.public_visits || 0) / maxValue) * 100}%`;
    track.appendChild(bar);
    const value = document.createElement("span");
    value.className = "trend-value";
    value.textContent = String(item.public_visits || 0);
    row.append(label, track, value);
    container.appendChild(row);
  });
}

function renderFeatures(rows) {
  const body = document.getElementById("feature-table-body");
  body.replaceChildren();
  if (!rows.length) {
    body.appendChild(emptyRow(2));
    return;
  }
  rows.forEach((item) => {
    const row = document.createElement("tr");
    row.append(textCell(item.event_name), textCell(item.uses));
    body.appendChild(row);
  });
}

function renderDownloads(rows) {
  const paperBody = document.getElementById("paper-table-body");
  const dataBody = document.getElementById("data-table-body");
  paperBody.replaceChildren();
  dataBody.replaceChildren();
  const paper = rows.filter((item) => item.category === "paper");
  const data = rows.filter((item) => item.category === "data");

  if (!paper.length) paperBody.appendChild(emptyRow(3));
  paper.forEach((item) => {
    const row = document.createElement("tr");
    row.append(
      textCell(item.document_version),
      textCell(item.file_format?.toUpperCase()),
      textCell(item.downloads),
    );
    paperBody.appendChild(row);
  });

  if (!data.length) dataBody.appendChild(emptyRow(4));
  data.forEach((item) => {
    const row = document.createElement("tr");
    row.append(
      textCell(item.dataset),
      textCell(item.dataset_year),
      textCell(item.granularity),
      textCell(item.downloads),
    );
    dataBody.appendChild(row);
  });
}

function renderExclusions(rows) {
  const body = document.getElementById("exclusion-table-body");
  body.replaceChildren();
  if (!rows.length) {
    body.appendChild(emptyRow(2));
    return;
  }
  rows.forEach((item) => {
    const row = document.createElement("tr");
    row.append(textCell(item.exclusion_reason), textCell(item.events));
    body.appendChild(row);
  });
}

function renderInternalStatus(data) {
  const status = document.getElementById("internal-status");
  const mark = document.getElementById("mark-internal");
  const unmark = document.getElementById("unmark-internal");
  if (!data.internal_exclusion_ready) {
    status.textContent = "Internal-browser signing is not configured.";
    mark.disabled = true;
    unmark.disabled = true;
    return;
  }
  status.textContent = data.internal_browser
    ? "This browser is marked internal and excluded from public totals."
    : "This browser currently counts as public activity.";
  mark.disabled = data.internal_browser;
  unmark.disabled = !data.internal_browser;
}

async function loadDashboard() {
  const endDate = endDateInput.value;
  const days = daysSelect.value;
  message.className = "usage-message";
  message.textContent = "Loading usage…";
  try {
    const response = await fetch(
      `/api/admin/usage/summary?end_date=${encodeURIComponent(endDate)}&days=${encodeURIComponent(days)}`,
      { cache: "no-store", credentials: "same-origin" },
    );
    if (!response.ok) throw new Error(`Dashboard request failed (${response.status})`);
    const data = await response.json();
    document.getElementById("selected-day-label").textContent =
      `${data.selected_day.date} · ${data.reporting_timezone}`;
    renderMetrics("selected-day-cards", data.selected_day);
    renderMetrics("cumulative-cards", data.cumulative);
    renderComparison(data.previous_day, data.selected_day);
    renderFeatures(data.features);
    renderDownloads(data.downloads);
    renderExclusions(data.exclusions);
    renderInternalStatus(data);
    const period = data.tracking_period;
    const trendRows = period.first_date
      ? data.daily.filter((row) => row.date >= period.first_date)
      : [];
    renderTrend(trendRows);
    document.getElementById("cumulative-period-label").textContent = period.first_date
      ? `Since tracking began · ${period.first_date}`
      : "Since tracking began";
    document.getElementById("tracking-period").textContent = period.first_date
      ? `Stored activity: ${period.first_date} through ${period.latest_date}`
      : "No activity has been stored yet";
    const startDate = data.start_date;
    exportLink.href =
      `/api/admin/usage/export.csv?start_date=${encodeURIComponent(startDate)}&end_date=${encodeURIComponent(data.end_date)}`;
    message.textContent = data.tracking_enabled
      ? `Showing ${data.days} days ending ${data.end_date}.`
      : "Tracking is currently disabled; configure the session secret before collection.";
  } catch (error) {
    message.className = "usage-message error";
    message.textContent = error instanceof Error ? error.message : "Unable to load usage.";
  }
}

async function updateInternalBrowser(method) {
  const response = await fetch("/api/admin/usage/internal-browser", {
    method,
    headers: { "X-Biochar-Usage-Admin": "1" },
    credentials: "same-origin",
    cache: "no-store",
  });
  if (!response.ok) throw new Error(`Internal-browser update failed (${response.status})`);
  await loadDashboard();
}

document.getElementById("usage-refresh").addEventListener("click", loadDashboard);
document.getElementById("mark-internal").addEventListener("click", () => {
  void updateInternalBrowser("POST").catch((error) => {
    message.className = "usage-message error";
    message.textContent = error.message;
  });
});
document.getElementById("unmark-internal").addEventListener("click", () => {
  void updateInternalBrowser("DELETE").catch((error) => {
    message.className = "usage-message error";
    message.textContent = error.message;
  });
});

void loadDashboard();
