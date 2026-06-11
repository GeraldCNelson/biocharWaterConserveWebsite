// @ts-check

/**
 * @param {...any} args
 * @returns {void}
 */
export function debugLog(...args) {
  console.log(...args);
}

/**
 * @param {string} title
 * @param {() => void} callback
 * @returns {void}
 */
export function debugGroup(title, callback) {
  console.groupCollapsed(title);
  try {
    callback();
  } finally {
    console.groupEnd();
  }
}