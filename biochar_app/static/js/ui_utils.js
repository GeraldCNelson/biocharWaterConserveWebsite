// @ts-check

/**
 * Return a select dropdown value by element id.
 * @param {string} id
 * @param {boolean} [parseAsInt=false]
 * @returns {string | number | null}
 */
export function getDropdownValue(id, parseAsInt = false) {
    const el = /** @type {HTMLSelectElement | null} */ (document.getElementById(id));
    if (!el) {
        console.warn(`⚠️ getDropdownValue: Element not found for id: ${id}`);
        return null;
    }

    const value = el.value;
    return parseAsInt ? parseInt(value, 10) : value;
}

/**
 * Show an alert to the user.
 * @param {string} message
 */
export function showAlert(message) {
    console.warn("⚠️ ALERT:", message);
    alert(message);
}

/**
 * Get the value of an input element by id.
 * @param {string} id
 * @returns {string}
 */
export function getInputValue(id) {
    const el = /** @type {HTMLInputElement | null} */ (document.getElementById(id));
    return el ? el.value : "";
}

/**
 * Set the value of an input element by id.
 * @param {string} id
 * @param {string} value
 */
export function setInputValue(id, value) {
    const el = /** @type {HTMLInputElement | null} */ (document.getElementById(id));
    if (el) el.value = value;
}

/**
 * Safely get any element by id.
 * @param {string} id
 * @returns {HTMLElement | null}
 */
export function getElementByIdSafe(id) {
    const el = document.getElementById(id);
    if (!el) {
        console.error(`❌ Element not found: ${id}`);
    }
    return el;
}

/**
 * Format a numeric value for display.
 * @param {number | string | null | undefined} value
 * @returns {string}
 */
export function formatValue(value) {
    if (value === null || value === undefined || value === "") return "NA";

    const num = typeof value === "number" ? value : Number(value);
    return Number.isNaN(num) ? "NA" : num.toFixed(4);
}

/**
 * Detect whether the current device is likely mobile.
 * @returns {boolean}
 */
export function isMobileDevice() {
    return /Mobi|Android|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i
        .test(navigator.userAgent);
}