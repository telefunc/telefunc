export { MemberMeta }
export type { MetaRewinds }

import { assignAttributes } from './model.js'
import type { MemberMetaUpdate } from './protocol.js'
import type { ParticipantMeta } from './types.js'

/** Merges held past this many keys, or past the meta's own size, are folded into the object, so held merges stay
 *  within a constant factor of the meta. */
const MEMBER_META_MERGES_HELD_MIN = 64

/** How many times a failover rewound a room's meta seqs. Each member's meta sharing it reads its seq as forgotten after
 *  one, so a rewind costs the same at any member count. */
type MetaRewinds = { count: number }
const NEVER_REWOUND: MetaRewinds = Object.freeze({ count: 0 })

/** A member's meta as a view keeps it: its updates apply in `seq` order, a merge in what it changed, and the object is
 *  built when read. What it is given is owned already, as an API argument copied or a frame or record decoded. */
class MemberMeta {
  private _object: ParticipantMeta
  private _keys: number
  /** Merges applied since `_object` was built, in order. */
  private _merges: ParticipantMeta[] | null = null
  private _mergedKeys = 0
  private _seq: number
  private readonly _rewinds: MetaRewinds
  private _rewindsSeen: number

  constructor({ meta, seq, rewinds = NEVER_REWOUND }: { meta: ParticipantMeta; seq: number; rewinds?: MetaRewinds }) {
    this._object = Object.freeze(meta)
    this._keys = Object.keys(this._object).length
    this._seq = seq
    this._rewinds = rewinds
    this._rewindsSeen = rewinds.count
  }

  /** -1 after a rewind until the next whole meta. */
  get seq(): number {
    return this._rewindsSeen === this._rewinds.count ? this._seq : -1
  }

  get object(): ParticipantMeta {
    if (this._merges !== null) this._fold(this._merges)
    return this._object
  }

  private _fold(merges: ParticipantMeta[]): void {
    const next: ParticipantMeta = { ...this._object }
    let keys = this._keys
    for (const attrs of merges) keys += assignAttributes(next, attrs)
    this._object = Object.freeze(next)
    this._keys = keys
    this._merges = null
    this._mergedKeys = 0
  }

  /** Whether a sender's stamp at `seq` is newer. None is while the seq is forgotten: a stamp from before a failover may
   *  carry a write it lost. */
  newerStamp(seq: number): boolean {
    const current = this.seq
    return current >= 0 && seq > current
  }

  /** A failover rewound the seq: any whole meta applies next, and a merge waits for one. */
  forget(): void {
    this._seq = -1
  }

  /** `gap` for a merge past the next seq: an update before it never arrived. */
  apply(update: MemberMetaUpdate): 'applied' | 'stale' | 'gap' {
    const seq = this.seq
    if (update.seq <= seq) return 'stale'
    if ('meta' in update) {
      this._object = Object.freeze(update.meta)
      this._keys = Object.keys(this._object).length
      this._merges = null
      this._mergedKeys = 0
      this._setSeq(update.seq)
      return 'applied'
    }
    if (update.seq > seq + 1) return 'gap'
    this._merges ??= []
    this._merges.push(update.attrs)
    this._mergedKeys += Object.keys(update.attrs).length
    this._setSeq(update.seq)
    if (this._mergedKeys > Math.max(MEMBER_META_MERGES_HELD_MIN, this._keys)) this._fold(this._merges)
    return 'applied'
  }

  private _setSeq(seq: number): void {
    this._seq = seq
    this._rewindsSeen = this._rewinds.count
  }
}
