// config.js

// Mapping of DOM container IDs to markdown files served by the backend.
export const MARKDOWN_FILES = {
  "intro-content": "/markdown/intro.md",
  "experiment-content": "/markdown/experimentDesign.md",
  "tech-content": "/markdown/techDetails.md",
  "modal-main-help": "/markdown/help_main.md",
  "modal-summary-help": "/markdown/help_summary.md",
};

// Simple debug flag for client-side logging.
export const DEBUG =
  window.location.hostname === "localhost" ||
  window.location.hostname === "127.0.0.1";

/**
 * Fallback only. Normally the backend sends defaults.unitSystem.
 */
export const FALLBACK_UNIT_SYSTEM = "us";
