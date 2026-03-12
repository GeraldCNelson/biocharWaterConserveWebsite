export function debugLog(...args) {
  console.log(...args);
}

export function debugGroup(title, callback) {
  console.groupCollapsed(title);
  try {
    callback();
  } finally {
    console.groupEnd();
  }
}