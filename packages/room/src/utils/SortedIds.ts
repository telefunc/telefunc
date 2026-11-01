export { SortedIds, compareCodePoints }

// A chunk this long is split in two, so an add or a delete moves at most this many ids.
const CHUNK_IDS_MAX = 1024
const CHUNK_IDS_HALF = CHUNK_IDS_MAX / 2

/** Ids in code point order, held in chunks: an add or a delete costs a binary search and one chunk's move, at any size. */
class SortedIds {
  private readonly chunks: string[][] = []

  constructor(ids: Iterable<string>) {
    const sorted = [...ids].sort(compareCodePoints)
    for (let at = 0; at < sorted.length; at += CHUNK_IDS_HALF) this.chunks.push(sorted.slice(at, at + CHUNK_IDS_HALF))
  }

  add(id: string): void {
    if (this.chunks.length === 0) {
      this.chunks.push([id])
      return
    }
    const c = Math.min(this.chunkOf(id), this.chunks.length - 1)
    const chunk = this.chunks[c]!
    const at = positionOf(chunk, id)
    if (chunk[at] === id) return
    chunk.splice(at, 0, id)
    if (chunk.length > CHUNK_IDS_MAX) this.chunks.splice(c + 1, 0, chunk.splice(CHUNK_IDS_HALF))
  }

  delete(id: string): void {
    const c = this.chunkOf(id)
    const chunk = this.chunks[c]
    if (chunk === undefined) return
    const at = positionOf(chunk, id)
    if (chunk[at] !== id) return
    chunk.splice(at, 1)
    if (chunk.length === 0) this.chunks.splice(c, 1)
  }

  /** The ids from `start` on, in order. */
  *from(start: string): Generator<string> {
    const first = this.chunkOf(start)
    for (let c = first; c < this.chunks.length; c++) {
      const chunk = this.chunks[c]!
      for (let at = c === first ? positionOf(chunk, start) : 0; at < chunk.length; at++) yield chunk[at]!
    }
  }

  /** The first chunk whose last id is at or past `id`, or the chunk count if none is. */
  private chunkOf(id: string): number {
    let low = 0
    let high = this.chunks.length
    while (low < high) {
      const mid = (low + high) >> 1
      if (compareCodePoints(this.chunks[mid]!.at(-1)!, id) < 0) low = mid + 1
      else high = mid
    }
    return low
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

/** Where `id` is in `ids`, which are in code point order, or where it would go. */
function positionOf(ids: readonly string[], id: string): number {
  let low = 0
  let high = ids.length
  while (low < high) {
    const mid = (low + high) >> 1
    if (compareCodePoints(ids[mid]!, id) < 0) low = mid + 1
    else high = mid
  }
  return low
}
