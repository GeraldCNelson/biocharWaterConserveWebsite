// debug_plotly_layout.js
/**
 * Plotly layout diagnostic helper
 *
 * Usage:
 *   1. Open browser dev tools
 *   2. Paste this script into the console
 *   3. Run to compare raw vs ratio plot geometry
 *
 * Purpose:
 *   Detect legend-width or margin mismatches that cause
 *   raw and ratio plots to misalign horizontally.
 */

If plotAreaWidths differ, the legends or margins are misaligned.

(() => {
  const rect = (sel) => document.querySelector(sel)?.getBoundingClientRect();

  console.table({
    plotAreaWidths: {
      raw: rect("#plot-1 .nsewdrag")?.width,
      ratio: rect("#plot-2 .nsewdrag")?.width
    },
    legendWidths: {
      raw: rect("#plot-1 .legend")?.width,
      ratio: rect("#plot-2 .legend")?.width
    },
    plotDivWidths: {
      raw: rect("#plot-1")?.width,
      ratio: rect("#plot-2")?.width
    }
  });
})();