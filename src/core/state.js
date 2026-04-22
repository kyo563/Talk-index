export function text(value) {
  return String(value || "").trim();
}

export function createInitialState() {
  return {
    search: "",
    videos: [],
    talks: [],
    favorites: new Set(),
  };
}

export function createRefs(doc = document) {
  return {
    search: doc.getElementById("search"),
    notice: doc.getElementById("notice"),
    error: doc.getElementById("error"),
    results: doc.getElementById("results"),
  };
}
