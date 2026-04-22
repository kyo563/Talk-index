export function filterBySearch(items, query, pickText) {
  const q = String(query || "").trim().toLowerCase();
  if (!q) return items;
  return items.filter((item) => String(pickText(item) || "").toLowerCase().includes(q));
}
