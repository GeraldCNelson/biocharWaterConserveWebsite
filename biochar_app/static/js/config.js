// config.js

// Mapping of DOM container IDs to markdown files served by the backend.
/**
 * Fetch Markdown container-id → URL mapping from the backend.
 *
 * Source of truth is markdown_config.build_markdown_mapping() in Python.
 */
export async function fetchMarkdownFiles() {
  const resp = await fetch("/api/markdown_files");
  if (!resp.ok) {
    console.error("❌ Failed to load markdown mapping, status", resp.status);
    throw new Error("Failed to load markdown mapping");
  }
  return await resp.json();
}

// Simple debug flag for client-side logging.
export const DEBUG =
  window.location.hostname === "localhost" ||
  window.location.hostname === "127.0.0.1";

/**
 * Fallback only. Normally the backend sends defaults.unitSystem.
 */
export const FALLBACK_UNIT_SYSTEM = "us";
