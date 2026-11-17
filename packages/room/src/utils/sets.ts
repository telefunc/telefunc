export { toggle, hasEntry, toggleEntry, runAll, setChange, withChange }
export type { SetChange }
import { getOrCreate } from 'telefunc/__internal'

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

/** The values a set gained (`add`) and lost (`remove`), each where there are any. */
type SetChange<T> = { add?: T[]; remove?: T[] }

/** What changed from `was` to `now`. */
function setChange<T>(was: ReadonlySet<T> | undefined, now: ReadonlySet<T>): SetChange<T> {
  const change: SetChange<T> = {}
  const add = [...now].filter((value) => !was?.has(value))
  const remove = [...(was ?? [])].filter((value) => !now.has(value))
  if (add.length > 0) change.add = add
  if (remove.length > 0) change.remove = remove
  return change
}

/** `was` with `change` applied, as a new set. */
function withChange<T>(was: ReadonlySet<T> | undefined, { add, remove }: SetChange<T> = {}): Set<T> {
  const now = new Set(was)
  for (const value of add ?? []) now.add(value)
  for (const value of remove ?? []) now.delete(value)
  return now
}
