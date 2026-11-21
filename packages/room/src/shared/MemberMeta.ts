export { MemberMeta, assignAttributes, sameValue }
export type { MetaRewinds, MetaWrite }

import { stringify } from '@brillout/json-serializer/stringify'
import { freezeMeta } from './freezeMeta.js'
import type { MemberMetaUpdate } from './protocol.js'
import type { ParticipantMeta } from '../types.js'

/** How many times a failover rewound a room's meta seqs. Each member's meta sharing it reads its seq as forgotten after
 *  one, so a rewind costs the same at any member count. */
type MetaRewinds = { count: number }
const NEVER_REWOUND: MetaRewinds = Object.freeze({ count: 0 })

/** A whole meta, or a merge of `attrs` (see `assignAttributes`). */
type MetaWrite = { meta: ParticipantMeta } | { attrs: ParticipantMeta }

type MemberMetaOptions = { meta: ParticipantMeta; seq: number; rewinds?: MetaRewinds; connected?: boolean }

/** A member's meta and connection as a view keeps them: its updates apply in `seq` order, a merge in the keys it
 *  changes, and the object is built when read. What it is given is owned already, as an API argument copied or a frame
 *  or record decoded. */
class MemberMeta {
  private _object: ParticipantMeta
  /** The values merges set since `_object` was built, by key; a key `_object` lacks goes after its keys, in this
   *  order. */
  private setKeys: Map<string, unknown> | null = null
  /** The keys of `_object` merges removed since it was built; one set again goes after its keys. */
  private removedKeys: Set<string> | null = null
  private _seq: number
  private readonly rewinds: MetaRewinds
  private rewindsSeen: number
  private _connected: boolean

  constructor({ meta, seq, rewinds = NEVER_REWOUND, connected = true }: MemberMetaOptions) {
    this._connected = connected
    this._object = freezeMeta(meta)
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
    if (this.setKeys !== null || this.removedKeys !== null) this.fold()
    return this._object
  }

  private fold(): void {
    const next: ParticipantMeta = { ...this._object }
    for (const key of this.removedKeys ?? []) delete next[key]
    for (const [key, value] of this.setKeys ?? []) setKey(next, key, value)
    this.setKeys = null
    this.removedKeys = null
    // Merges that undid one another leave the object, as readers tell a change by identity.
    if (!sameValue(next, this._object)) this._object = freezeMeta(next)
  }

  /** Whether `write` leaves the meta another value. */
  changedBy(write: MetaWrite): boolean {
    if ('meta' in write) return !sameValue(write.meta, this.object)
    return Object.entries(write.attrs).some(([key, value]) =>
      this.has(key) ? value === undefined || !sameValue(value, this.valueOf(key)) : value !== undefined,
    )
  }

  private has(key: string): boolean {
    return this.setKeys?.has(key) || (Object.hasOwn(this._object, key) && !this.removedKeys?.has(key))
  }

  /** The value of a key `has()`. */
  private valueOf(key: string): unknown {
    return this.setKeys?.has(key) ? this.setKeys.get(key) : this._object[key]
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

  /** `gap` for a change past the next seq: an update before it never arrived. `unchanged` for one that leaves the meta
   *  and the connection as they were, whose seq still applies. */
  apply(update: MemberMetaUpdate): 'changed' | 'unchanged' | 'stale' | 'gap' {
    const seq = this.seq
    if (update.seq <= seq) return 'stale'
    let changed: boolean
    if ('meta' in update) {
      const metaChanged = this.changedBy(update)
      if (metaChanged) this._object = freezeMeta(update.meta)
      const connected = update.disconnected !== true
      changed = metaChanged || connected !== this._connected
      this._connected = connected
    } else if (update.seq > seq + 1) {
      return 'gap'
    } else if ('connected' in update) {
      changed = update.connected !== this._connected
      this._connected = update.connected
    } else {
      changed = this.changedBy(update)
      if (changed) this.merge(update.attrs)
    }
    this._seq = update.seq
    this.rewindsSeen = this.rewinds.count
    return changed ? 'changed' : 'unchanged'
  }

  private merge(attrs: ParticipantMeta): void {
    for (const [key, value] of Object.entries(attrs)) {
      if (value === undefined) {
        this.setKeys?.delete(key)
        if (Object.hasOwn(this._object, key)) (this.removedKeys ??= new Set()).add(key)
      } else {
        ;(this.setKeys ??= new Map()).set(key, value)
      }
    }
  }
}

/** Two values are one when they serialize the same: two objects with their keys in another order are two. */
function sameValue(a: unknown, b: unknown): boolean {
  return stringify(a) === stringify(b)
}

/** Assign `attrs` onto `target` per key. A value of `undefined` deletes its key (the serializer preserves `undefined`
 *  on the wire). */
function assignAttributes(target: ParticipantMeta, attrs: ParticipantMeta): void {
  for (const [key, value] of Object.entries(attrs)) {
    if (value === undefined) delete target[key]
    else setKey(target, key, value)
  }
}

/** A key already there keeps its place; `__proto__` is a key like any other. */
function setKey(target: ParticipantMeta, key: string, value: unknown): void {
  Object.defineProperty(target, key, { value, enumerable: true, configurable: true, writable: true })
}
