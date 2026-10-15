export { toggle, hasEntry, toggleEntry, runAll }

function toggle<T>(set: Set<T>, value: T, on: boolean): void {
  if (on) set.add(value)
  else set.delete(value)
}
function hasEntry<K, V>(entries: Map<K, Set<V>>, key: K, value: V): boolean {
  return entries.get(key)?.has(value) === true
}
function toggleEntry<K, V>(entries: Map<K, Set<V>>, key: K, value: V, on: boolean): void {
  let values = entries.get(key)
  if (on) {
    if (!values) entries.set(key, (values = new Set()))
    values.add(value)
  } else if (values?.delete(value) && values.size === 0) entries.delete(key)
}
/** Runs every step now. */
function runAll(steps: Iterable<unknown>): void {
  for (const _ of steps);
}
