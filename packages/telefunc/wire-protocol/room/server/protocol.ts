// Room storage records and the lane envelopes only instances read.
export type {
  RoomConfigRecord,
  HoldConfig,
  RoomMemberRecord,
  DemandEntry,
  RoomDemandPage,
  RoomDataEnvelope,
  RoomDmAckEnvelope,
}

import type { DmReply, SenderStamp } from '../protocol.js'
import type { RoomMeta } from '../types.js'

/** The head's config. `meta` is the one the room was created with, until its first meta write (see `meta.ts`). `inc`
 *  is random, not a counter: a room recreated after its tombstone lapses can't reuse an id a stale handle holds. */
type RoomConfigRecord = {
  meta: RoomMeta
  inc: string
  /** Its hold timeouts (see `lifecycle.ts`); a room that never closes on its own has none. */
  hold: HoldConfig | null
}

type HoldConfig = { emptyTimeout: number; departureTimeout: number }

/** A member cell. It lives while the session that joined it does (see `session.ts`). Its meta is a cell per key in
 *  generation `metaGen` (see `memberMetaGenPrefix`). */
type RoomMemberRecord = {
  joinedAt: number
  /** The session hosting the member. */
  session: string
  /** Issued by the member's single owner, counting its meta writes; orders `p-meta` events. */
  metaSeq: number
  /** The `metaSeq` of the replace that wrote the meta's keys, 0 for the join's. */
  metaGen: number
  identity?: string
  /** Off-presence (`join({ hidden: true })`): routable, but not in count, roster or presence events. */
  hidden?: boolean
}

/** What an instance receives: one track of a member, every track of a member (no `track`), or one track of every
 *  member but `except`, told once, then each member it starts (`on`) or stops being told without (`except: true`). A
 *  track it fetches by member (`byMember`) covers only the members it tells `{ member, byMember }` for, each told once
 *  that member's tracks reach it. `''` is the default track. */
type DemandEntry =
  | { member: string; track: string }
  | { member: string }
  | { member: string; byMember: true }
  | { track: string; except: readonly string[]; byMember?: true }
  | { track: string; member: string; except: true }

/** Demand between instances, on the demand lane: entries one instance starts or stops receiving, in the order it told
 *  them, at most `ROOM_DEMAND_PAGE_ENTRIES`, its `page`th page. Its entries from `{ again: true }` to
 *  `{ again: false }` are all it receives. */
type RoomDemandPage = {
  __r: 'demand'
  instance: string
  page: number
  wants: Array<({ on: boolean } & DemandEntry) | { again: boolean }>
}

/** A member's message on the lane; its order rides the transport frame. Stamped, so a receiver behind on the roster
 *  still names the sender. */
type RoomDataEnvelope = { __r: 'data'; from: string; data: unknown } & SenderStamp
/** An ack DM's reply, on the original sender's inbox lane. */
type RoomDmAckEnvelope = { __r: 'dm-ack'; to: string; ackId: string } & DmReply
