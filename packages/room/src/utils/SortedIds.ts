export { SortedIds, compareCodePoints }

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

/** Code point order: UTF-16 code unit order, but for a surrogate, which sorts after every unit from 0xE000 on. */
function compareCodePoints(left: string, right: string): number {
  const length = Math.min(left.length, right.length)
  for (let i = 0; i < length; i++) {
    const a = left.charCodeAt(i)
    const b = right.charCodeAt(i)
    if (a !== b) return codePointRank(a) - codePointRank(b)
  }
  return left.length - right.length
}

/** A code unit's place in code point order: the surrogates, 0xD800 to 0xDFFF, move past 0xE000 to 0xFFFF. */
function codePointRank(unit: number): number {
  return unit < 0xd800 ? unit : unit < 0xe000 ? unit + 0x2000 : unit - 0x800
}

/** Where `id` is in `ids`, which are in code point order, or where it would go, searching from `low` on. */
function positionOf(ids: readonly string[], id: string, low = 0): number {
  let high = ids.length
  while (low < high) {
    const mid = (low + high) >> 1
    // Below `high`, `mid` reads an id.
    const at = ids[mid]
    if (at === undefined || compareCodePoints(at, id) >= 0) high = mid
    else low = mid + 1
  }
  return low
}
