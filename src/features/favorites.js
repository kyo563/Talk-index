export function toggleFavorite(setLike, id) {
  if (setLike.has(id)) {
    setLike.delete(id);
    return false;
  }
  setLike.add(id);
  return true;
}

export function isFavorite(setLike, id) {
  return setLike.has(id);
}
