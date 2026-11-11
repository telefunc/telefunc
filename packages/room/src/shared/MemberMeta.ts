export { MemberMeta, assignAttributes }
export type { MetaRewinds }

import { freezeMeta } from './freezeMeta.js'
import type { MemberMetaUpdate } from './protocol.js'
import type { ParticipantMeta } from '../types.js'

/** Merges held past this many keys, or past the meta's own size, are folded into the object, so held merges stay
 *  within a constant factor of the meta. */
const MEMBER_META_MERGES_HELD_MIN = 64

/** How many times a failover rewound a room's meta seqs. Each member's meta sharing it reads its seq as forgotten after
 *  one, so a rewind costs the same at any member count. */
type MetaRewinds = { count: number }
const NEVER_REWOUND: MetaRewinds = Object.freeze({ count: 0 })

type MemberMetaOptions = { meta: ParticipantMeta; seq: number; rewinds?: MetaRewinds; connected?: boolean }

/** A member's meta and connection as a view keeps them: its updates apply in `seq` order, a merge in what it changed,
 *  and the object is built when read. What it is given is owned already, as an API argument copied or a frame or record
 *  decoded. */
class MemberMeta {
  private _object: ParticipantMeta
  private keys: number
  /** Merges applied since `_object` was built, in order. */
  private merges: ParticipantMeta[] | null = null
  private mergedKeys = 0
  private _seq: number
  private readonly rewinds: MetaRewinds
  private rewindsSeen: number
  private _connected: boolean

  constructor({ meta, seq, rewinds = NEVER_REWOUND, connected = true }: MemberMetaOptions) {
    this._connected = connected
    this._object = freezeMeta(meta)
    this.keys = Object.keys(this._object).length
    this._seq = seq
    this.rewinds = rewinds
    this.rewindsSeen = rewinds.count
  }

  /** -1 after a rewind until the next whole meta. */
  get seq(): number {
    return this.rewindsSeen === this.rewinds.count ? this._seq : -1
  }

  get connected(): boolean {
    return this._connected
  }

  get object(): ParticipantMeta {
    if (this.merges !== null) this.fold(this.merges)
    return this._object
  }

  private fold(merges: ParticipantMeta[]): void {
    const next: ParticipantMeta = { ...this._object }
    let keys = this.keys
    for (const attrs of merges) keys += assignAttributes(next, attrs)
    this._object = freezeMeta(next)
    this.keys = keys
    this.merges = null
    this.mergedKeys = 0
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

  /** `gap` for a change past the next seq: an update before it never arrived. */
  apply(update: MemberMetaUpdate): 'applied' | 'stale' | 'gap' {
    const seq = this.seq
    if (update.seq <= seq) return 'stale'
    if ('meta' in update) {
      this._object = freezeMeta(update.meta)
      this.keys = Object.keys(this._object).length
      this.merges = null
      this.mergedKeys = 0
      this._connected = update.disconnected !== true
      this.setSeq(update.seq)
      return 'applied'
    }
    if (update.seq > seq + 1) return 'gap'
    if ('connected' in update) {
      this._connected = update.connected
      this.setSeq(update.seq)
      return 'applied'
    }
    this.merges ??= []
    this.merges.push(update.attrs)
    this.mergedKeys += Object.keys(update.attrs).length
    this.setSeq(update.seq)
    if (this.mergedKeys > Math.max(MEMBER_META_MERGES_HELD_MIN, this.keys)) this.fold(this.merges)
    return 'applied'
  }

  private setSeq(seq: number): void {
    this._seq = seq
    this.rewindsSeen = this.rewinds.count
  }
}

/** Assign `attrs` onto `target` per key, returning the change in its key count. A value of `undefined` deletes its
 *  key (the serializer preserves `undefined` on the wire). */
function assignAttributes(target: ParticipantMeta, attrs: ParticipantMeta): number {
  let keys = 0
  for (const [key, value] of Object.entries(attrs)) {
    const had = Object.hasOwn(target, key)
    if (value === undefined) {
      if (had) keys--
      delete target[key]
      continue
    }
    if (!had) keys++
    Object.defineProperty(target, key, { value, enumerable: true, configurable: true, writable: true })
  }
  return keys
}
