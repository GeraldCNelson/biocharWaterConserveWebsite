export function getDropdownValue(id, parseAsInt = false) {
    const el = document.getElementById(id);
    if (!el) {
        console.warn(`⚠️ getDropdownValue: Element not found for id: ${id}`);
        return null;
    }
    const value = el.value;
    return parseAsInt ? parseInt(value, 10) : value;
}

export function getInputValue(id) {
    const el = document.getElementById(id);
    return el ? el.value : "";
}

export function setInputValue(id, value) {
    const el = document.getElementById(id);
    if (el) el.value = value;
}

export function getElementByIdSafe(id) {
    const el = document.getElementById(id);
    if (!el) {
        console.error(`❌ Element not found: ${id}`);
    }
    return el;
}
