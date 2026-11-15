// Anonymous, allowlisted feature events. The server stores no IP address or
// raw browser user-agent string with these events.

const TAB_EVENT_NAMES = new Map([
  ["#intro", "tab:intro"],
  ["#draft-report", "tab:draft-report"],
  ["#what-is-biochar", "tab:what-is-biochar"],
  ["#experiment", "tab:experiment"],
  ["#main", "tab:interactive-plots"],
  ["#summary", "tab:summary"],
  ["#gseason", "tab:growing-season"],
  ["#biochar-analysis", "tab:biochar-analysis"],
  ["#nir", "tab:nir"],
  ["#biomass-field", "tab:biomass-field"],
  ["#soilbio", "tab:soil-biology"],
  ["#soilchem", "tab:soil-chemistry"],
  ["#tech-details", "tab:technical-details"],
  ["#bulk-pane", "tab:bulk-downloads"],
  ["#glossary", "tab:glossary"],
  ["#acknowledgements", "tab:acknowledgements"],
]);

function sendFeatureEvent(eventName) {
  const body = JSON.stringify({ event_name: eventName });
  if (navigator.sendBeacon) {
    const accepted = navigator.sendBeacon(
      "/api/usage/event",
      new Blob([body], { type: "application/json" }),
    );
    if (accepted) return;
  }

  void fetch("/api/usage/event", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body,
    credentials: "same-origin",
    keepalive: true,
  }).catch(() => {
    // Usage reporting must never interfere with the public interface.
  });
}

document.addEventListener("shown.bs.tab", (event) => {
  const control = event.target;
  if (!(control instanceof Element)) return;
  const target = control.getAttribute("data-bs-target");
  const eventName = target ? TAB_EVENT_NAMES.get(target) : null;
  if (eventName) sendFeatureEvent(eventName);
});
