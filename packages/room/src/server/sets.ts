export { toggle, hasEntry, toggleEntry, runAll }

import { getOrCreate } from '../utils/getOrCreate.js'

function toggle<T>(set: Set<T>, value: T, on: boolean): void {
  if (on) set.add(value)
  else set.delete(value)
}

function hasEntry<K, V>(entries: Map<K, Set<V>>, key: K, value: V): boolean {
  return entries.get(key)?.has(value) === true
}

function toggleEntry<K, V>(entries: Map<K, Set<V>>, key: K, value: V, on: boolean): void {
  if (on) {
    getOrCreate(entries, key, () => new Set()).add(value)
    return
  }
  const values = entries.get(key)
  if (values?.delete(value) && values.size === 0) entries.delete(key)
}

/** Runs every step now. */
function runAll(steps: Iterable<unknown>): void {
  for (const _ of steps);
}
