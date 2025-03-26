/**
 * ✅ Markdown Loader Script
 * - Loads and renders markdown content dynamically into specified elements.
 */

/**
 * Load markdown content into a specified HTML element.
 * @param {string} elementId - The ID of the element where the markdown will be inserted.
 * @param {string} markdownUrl - The URL of the markdown file to fetch.
 */
async function loadMarkdownContent(elementId, markdownUrl) {
    console.log(`📖 Loading markdown into #${elementId} from ${markdownUrl}...`);

    try {
        const response = await fetch(markdownUrl);
        if (!response.ok) {
            throw new Error(`Failed to fetch markdown file: ${markdownUrl}`);
        }

        const markdownText = await response.text();
        document.getElementById(elementId).innerHTML = convertMarkdownToHtml(markdownText);

        console.log(`✅ Markdown successfully loaded into #${elementId}`);
    } catch (error) {
        console.error(`❌ Error loading markdown for #${elementId}:`, error);
        document.getElementById(elementId).innerHTML =
            `<p class="text-danger">Failed to load content from ${markdownUrl}</p>`;
    }
}

/**
 * Convert markdown text to HTML.
 * @param {string} markdown - The markdown text.
 * @returns {string} - The converted HTML.
 */
function convertMarkdownToHtml(markdown) {
    return markdown
        .replace(/^### (.*$)/gim, '<h3>$1</h3>')
        .replace(/^## (.*$)/gim, '<h2>$1</h2>')
        .replace(/^# (.*$)/gim, '<h1>$1</h1>')
        .replace(/\*\*(.*)\*\*/gim, '<b>$1</b>')
        .replace(/\*(.*)\*/gim, '<i>$1</i>')
        .replace(/!\[(.*?)\]\((.*?)\)/gim, "<img alt='$1' src='$2' />")
        .replace(/\[(.*?)\]\((.*?)\)/gim, "<a href='$2'>$1</a>")
        .replace(/\n/g, '<br>');
}