export function renderStatus(el, message = "", mode = "") {
  if (!el) return;
  el.textContent = message;
  if (mode) {
    el.dataset.mode = mode;
  } else {
    delete el.dataset.mode;
  }
}
