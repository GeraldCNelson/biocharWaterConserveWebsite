import { getElementByIdSafe } from "./ui_utils.js";

/**
 * 📖 Converts raw markdown text to HTML using the `marked` library.
 */
function convertMarkdownToHtml(markdownText) {
    return marked.parse(markdownText);  // marked must be globally loaded
}

/**
 * 📥 Fetch and render markdown content into the target container.
 * @param {string} elementId - The ID of the DOM element to insert content into.
 * @param {string} markdownUrl - The URL to fetch the markdown file from.
 */
export async function loadMarkdownContent(elementId, markdownUrl) {
    console.log(`📖 Loading markdown into #${elementId} from ${markdownUrl}...`);
    try {
        const response = await fetch(markdownUrl);
        if (!response.ok) {
            throw new Error(`Failed to fetch markdown file: ${markdownUrl}`);
        }

        const markdownText = await response.text();
        const container = getElementByIdSafe(elementId);
        container.innerHTML = convertMarkdownToHtml(markdownText);

        console.log(`✅ Markdown successfully loaded into #${elementId}`);
    } catch (error) {
        console.error(`❌ Error loading markdown for #${elementId}:`, error);
        const fallback = getElementByIdSafe(elementId);
        fallback.innerHTML = `<p class="text-danger">Failed to load content from ${markdownUrl}</p>`;
    }
}
