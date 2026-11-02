export { RoomMetaState, compareMetaEntries, metaEntriesAt, metaObject }

import type { RoomMetaEntry, RoomMetaUpdate } from './protocol.js'
import type { RoomMeta } from '../types.js'

/** Keys go by place, then by key: the order every read of the stored meta lists them in. */
function compareMetaEntries(a: RoomMetaEntry, b: RoomMetaEntry): number {
  return a[2] - b[2] || a[3] - b[3] || (a[0] < b[0] ? -1 : 1)
}

/** A whole meta's keys, placed at `seq` by their index: what a replace writes, or the created meta at seq 0. */
function metaEntriesAt(meta: RoomMeta, seq: number): RoomMetaEntry[] {
  return Object.entries(meta).map(([key, value], index) => [key, value, seq, index])
}

function metaObject(entries: Iterable<RoomMetaEntry>): RoomMeta {
  return Object.freeze(Object.fromEntries(Array.from(entries, ([key, value]) => [key, value])))
}

type RoomMetaStateOptions = { entries: readonly RoomMetaEntry[]; seq: number }

/** A room meta as a view keeps it: its updates apply in `seq` order, each in what it changed, and the plain object is
 *  built when read. */
class RoomMetaState {
  /** In key order while `ordered`. */
  private _entries: Map<string, RoomMetaEntry>
  /** False once a change placed a key before the last one added, which concurrent writes can do: the next read
   *  orders. */
  private ordered = true
  private last: RoomMetaEntry | undefined
  private _object: RoomMeta | null = null
  private _seq: number
  /** Changes past the next seq, until the ones before them arrive or a whole meta covers them. */
  private readonly pendingUpdates = new Map<number, RoomMetaUpdate>()

  constructor({ entries, seq }: RoomMetaStateOptions) {
    this._entries = new Map(entries.map((entry) => [entry[0], entry]))
    this.last = entries.at(-1)
    this._seq = seq
  }

  /** How many writes the meta reflects. */
  get seq(): number {
    return this._seq
  }

  get waiting(): boolean {
    return this.pendingUpdates.size > 0
  }

  get object(): RoomMeta {
    return (this._object ??= metaObject(this.orderedEntries()))
  }

  entries(): RoomMetaEntry[] {
    return [...this.orderedEntries()]
  }

  /** A failover rewound the seq: any whole meta applies next, and a change waits for one. */
  forget(): void {
    this._seq = -1
    this.pendingUpdates.clear()
  }

  /** The updates `update` lets apply, in seq order: a whole meta at any newer seq, a change at the next one, then the
   *  waiting changes that follow. A change past the next seq waits. */
  apply(update: RoomMetaUpdate): RoomMetaUpdate[] {
    if (update.seq <= this._seq) return []
    if ('changes' in update && update.seq > this._seq + 1) {
      this.pendingUpdates.set(update.seq, update)
      return []
    }
    const applied = [update]
    this.applyOne(update)
    for (const seq of this.pendingUpdates.keys()) if (seq <= this._seq) this.pendingUpdates.delete(seq)
    for (
      let next = this.pendingUpdates.get(this._seq + 1);
      next !== undefined;
      next = this.pendingUpdates.get(this._seq + 1)
    ) {
      this.pendingUpdates.delete(next.seq)
      this.applyOne(next)
      applied.push(next)
    }
    return applied
  }

  private applyOne(update: RoomMetaUpdate): void {
    this._seq = update.seq
    this._object = null
    if ('meta' in update) {
      this._entries = new Map(update.meta.map((entry) => [entry[0], entry]))
      this.last = update.meta.at(-1)
      this.ordered = true
      return
    }
    for (const change of update.changes) {
      if (change.length === 1) this._entries.delete(change[0])
      else this.set(change)
    }
  }

  /** A key keeps its spot while its place holds; a key placed anew goes after the last one, out of order if it sorts
   *  before it. */
  private set(entry: RoomMetaEntry): void {
    const found = this._entries.get(entry[0])
    if (found !== undefined && found[2] === entry[2] && found[3] === entry[3]) {
      this._entries.set(entry[0], entry)
      return
    }
    if (found !== undefined) this._entries.delete(entry[0])
    if (this.last !== undefined && compareMetaEntries(entry, this.last) < 0) this.ordered = false
    else this.last = entry
    this._entries.set(entry[0], entry)
  }

  private orderedEntries(): Iterable<RoomMetaEntry> {
    if (!this.ordered) {
      // Mostly in order already, so the sort runs close to one pass.
      const entries = [...this._entries.values()].sort(compareMetaEntries)
      this._entries = new Map(entries.map((entry) => [entry[0], entry]))
      this.last = entries.at(-1)
      this.ordered = true
    }
    return this._entries.values()
  }
}
