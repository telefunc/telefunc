export function partition<T>(arr: readonly T[], predicate: (item: T) => boolean): [T[], T[]] {
  const pass: T[] = [],
    fail: T[] = []
  for (const item of arr) (predicate(item) ? pass : fail).push(item)
  return [pass, fail]
}
