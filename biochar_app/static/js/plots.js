// plots.js
//
// Public plotting API for the app.
// Other modules should import from this file, not from plot_utils.js directly.

import {
  fetchAndRenderPlot,
  renderMainPlots,
  wireMainPlotZoomSync,
  waitForAllDropdowns,
} from "./plot_utils.js";

export {
  fetchAndRenderPlot,
  renderMainPlots,
  wireMainPlotZoomSync,
  waitForAllDropdowns,
};

export async function updatePlot(plotType, plotDivId) {
  return fetchAndRenderPlot(plotType, plotDivId);
}

export function capitalize(s) {
  return String(s || "").charAt(0).toUpperCase() + String(s || "").slice(1);
}