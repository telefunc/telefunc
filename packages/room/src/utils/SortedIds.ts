export { SortedIds, compareCodePoints }

/** Ids in code point order. An add or a delete is recorded, and the next read merges the recorded ids in at once. */
class SortedIds {
  private ids: string[]
  private readonly added = new Set<string>()
  private readonly deleted = new Set<string>()

  constructor(ids: Iterable<string>) {
    this.ids = [...ids].sort(compareCodePoints)
  }

  /** `id` isn't held. */
  add(id: string): void {
    if (!this.deleted.delete(id)) this.added.add(id)
  }

  /** `id` is held. */
  delete(id: string): void {
    if (!this.added.delete(id)) this.deleted.add(id)
  }

  /** The ids from `start` on, in order. */
  *from(start: string): Generator<string> {
    const ids = this.merged()
    for (let at = positionOf(ids, start); at < ids.length; at++) yield ids[at]!
  }

  private merged(): string[] {
    if (this.added.size === 0 && this.deleted.size === 0) return this.ids
    const ids: string[] = []
    let at = 0
    for (const id of [...this.added, ...this.deleted].sort(compareCodePoints)) {
      const to = positionOf(this.ids, id, at)
      while (at < to) ids.push(this.ids[at++]!)
      // A deleted id is held at `to`.
      if (this.added.has(id)) ids.push(id)
      else at++
    }
    while (at < this.ids.length) ids.push(this.ids[at++]!)
    this.added.clear()
    this.deleted.clear()
    this.ids = ids
    return ids
  }
}

function compareCodePoints(left: string, right: string): number {
  for (let i = 0; i < left.length && i < right.length; ) {
    const a = left.codePointAt(i)!
    const b = right.codePointAt(i)!
    if (a !== b) return a - b
    i += a > 0xffff ? 2 : 1
  }
  return left.length - right.length
}

/** Where `id` is in `ids`, which are in code point order, or where it would go, searching from `low` on. */
function positionOf(ids: readonly string[], id: string, low = 0): number {
  let high = ids.length
  while (low < high) {
    const mid = (low + high) >> 1
    if (compareCodePoints(ids[mid]!, id) < 0) low = mid + 1
    else high = mid
  }
  return low
}
