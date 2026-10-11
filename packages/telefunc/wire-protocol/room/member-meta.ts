export { MemberMeta }

import { ownMetadata } from './model.js'
import type { MemberMetaUpdate } from './protocol.js'
import type { ParticipantMeta } from './types.js'

/** Merges held past this many keys, or past the meta's own size, are folded into the object, so held merges stay
 *  within a constant factor of the meta. */
const MEMBER_META_MERGES_HELD_MIN = 64

/** A member's meta as a view keeps it: its updates apply in `seq` order, a merge in what it changed, and the object is
 *  built when read. */
class MemberMeta {
  private _object: ParticipantMeta
  private _keys: number
  /** Merges applied since `_object` was built, in order. */
  private _merges: ParticipantMeta[] | null = null
  private _mergedKeys = 0
  private _seq: number

  constructor(meta: ParticipantMeta, seq: number) {
    this._object = ownMetadata(meta)
    this._keys = Object.keys(this._object).length
    this._seq = seq
  }

  get seq(): number {
    return this._seq
  }

  get object(): ParticipantMeta {
    const merges = this._merges
    if (merges === null) return this._object
    const next: ParticipantMeta = { ...this._object }
    let keys = this._keys
    for (const attrs of merges)
      for (const [key, value] of Object.entries(attrs)) {
        const had = Object.hasOwn(next, key)
        if (value === undefined) {
          if (had) keys--
          delete next[key]
          continue
        }
        if (!had) keys++
        Object.defineProperty(next, key, { value, enumerable: true, configurable: true, writable: true })
      }
    this._object = Object.freeze(next)
    this._keys = keys
    this._merges = null
    this._mergedKeys = 0
    return this._object
  }

  /** `gap` for a merge past the next seq: an update before it never arrived. */
  apply(update: MemberMetaUpdate): 'applied' | 'stale' | 'gap' {
    if (update.seq <= this._seq) return 'stale'
    if ('meta' in update) {
      this._object = ownMetadata(update.meta)
      this._keys = Object.keys(this._object).length
      this._merges = null
      this._mergedKeys = 0
      this._seq = update.seq
      return 'applied'
    }
    if (update.seq > this._seq + 1) return 'gap'
    const attrs = ownMetadata(update.attrs)
    ;(this._merges ??= []).push(attrs)
    this._mergedKeys += Object.keys(attrs).length
    this._seq = update.seq
    if (this._mergedKeys > Math.max(MEMBER_META_MERGES_HELD_MIN, this._keys)) void this.object
    return 'applied'
  }
}
