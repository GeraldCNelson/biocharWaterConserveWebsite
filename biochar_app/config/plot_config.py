"""Shared server-side configuration for interactive Plotly figures.

The Python builders are the authoritative source for base figure dimensions.
Browser-only responsive layout values cannot be imported from this module; they
are kept beside the code that applies them in:

    biochar_app/static/js/plot_utils.js

Frontend constants currently maintained there:

- ``FALLBACK_PLOT_HEIGHT``: used only when a server figure omits ``height``.
- ``DEFAULT_BOTTOM_MARGIN``: minimum browser-side Plotly bottom margin.
- ``DEFAULT_RIGHT_LEGEND_MARGIN``: space reserved for a right-side legend.

Keep ``FALLBACK_PLOT_HEIGHT`` equal to ``DEFAULT_PLOT_HEIGHT``. Normally the
frontend receives the authoritative value in ``plotData.layout.height``.
"""

from __future__ import annotations


DEFAULT_PLOT_HEIGHT = 400

# Metadata for maintainers; these values document the cross-language boundary
# without pretending that browser JavaScript can import Python configuration.
FRONTEND_PLOT_CONFIG_PATH = "biochar_app/static/js/plot_utils.js"
FRONTEND_PLOT_CONFIG_CONSTANTS = (
    "FALLBACK_PLOT_HEIGHT",
    "DEFAULT_BOTTOM_MARGIN",
    "DEFAULT_RIGHT_LEGEND_MARGIN",
)
