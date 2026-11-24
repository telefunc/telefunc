export { SortedIds, compareCodePoints }

import { assert } from './assert.js'

/** The pieces one `concat()` joins, well under the arguments a call takes in any engine. */
const CONCAT_BATCH = 8192

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
    // Past the last id, `ids[at]` is undefined.
    for (let at = positionOf(ids, start), id = ids[at]; id !== undefined; id = ids[++at]) yield id
  }

  /** The held ids with the recorded changes merged in: the runs between changes are sliced and joined natively. */
  private merged(): string[] {
    if (this.added.size === 0 && this.deleted.size === 0) return this.ids
    const pieces: string[][] = []
    let at = 0
    for (const id of [...this.added, ...this.deleted].sort(compareCodePoints)) {
      const to = positionOf(this.ids, id, at)
      pieces.push(this.ids.slice(at, to))
      at = to
      // A deleted id is held at `at`.
      if (this.added.has(id)) pieces.push([id])
      else at++
    }
    pieces.push(this.ids.slice(at))
    let ids: string[] = []
    // A batch at a time, so no call passes more arguments than an engine takes.
    for (let i = 0; i < pieces.length; i += CONCAT_BATCH) ids = ids.concat(...pieces.slice(i, i + CONCAT_BATCH))
    this.added.clear()
    this.deleted.clear()
    this.ids = ids
    return ids
  }
}

function compareCodePoints(left: string, right: string): number {
  for (let i = 0; i < left.length && i < right.length; ) {
    const a = left.codePointAt(i)
    const b = right.codePointAt(i)
    assert(a !== undefined && b !== undefined)
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
    const at = ids[mid]
    assert(at !== undefined)
    if (compareCodePoints(at, id) < 0) low = mid + 1
    else high = mid
  }
  return low
}
