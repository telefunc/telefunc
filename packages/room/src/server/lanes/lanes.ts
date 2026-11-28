export {
  CONTROL_LANE,
  DEMAND_LANE,
  SEMANTIC_LANE,
  acceptedOrThrow,
  commitFitsBeforeWriting,
  commitLane,
  controlLane,
  inboxOf,
  openConfig,
  readOpenConfig,
  configFromHead,
  decodeDemandPage,
  decodeCtrlEnvelope,
  decodeDataEnvelope,
  decodeInboxEnvelope,
  decodeRoomRecord,
  memberRecordOf,
  dmReplyOf,
  departureCauseOf,
  decodeRoomText,
  encodeDemandPage,
  encodeRoomRecord,
  handoffLost,
  lostReason,
  publishedBytes,
  withinRoomHorizon,
}
export type { CommitFitsBeforeWriting, CommitsOf, LaneCommit, SizedCommit }

import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { utf8ByteLength } from 'telefunc/__internal'
import { raceTimeout } from '../../utils/raceTimeout.js'
import type { RoomStore } from '../../backend/RoomStore.js'
import type {
  CommitAccepted,
  CommitKept,
  CommitOptions,
  CommitTarget,
  LaneId,
  RoomHead,
  StaleIncarnation,
} from '../../backend/contract.js'
import type { MemberSnapshot, RoomCtrlEnvelope, RoomDmEnvelope, WireLeaveCause } from '../../shared/protocol.js'
import {
  ctrlEnvelopeOf,
  dmEnvelopeOf,
  fieldsOf,
  leaveCauseOf,
  metaOf,
  numberOf,
  optionalText,
  roomFailureOf,
  textListOf,
  textOf,
} from '../../shared/readers.js'
import { decodeBinaryFrame } from '../../shared/binary.js'
import type {
  DemandEntry,
  WhoEntry,
  RoomConfigRecord,
  RoomDataEnvelope,
  RoomDemandPage,
  RoomDmAckEnvelope,
  RoomIncarnation,
  RoomMemberRecord,
} from '../protocol.js'
import { RoomError, participantGoneError, roomClosedError } from '../../shared/errors.js'
import { assert } from '../../utils/assert.js'
import { ROOM_HORIZON_MS } from '../constants.js'
import { reportDeliveryLost } from '../config.js'
import { memberIdOfCellKey } from '../store/cells.js'

const roomTextEncoder = new TextEncoder()
const roomTextDecoder = new TextDecoder()
const SEMANTIC_LANE = { kind: 'semantic' } as const satisfies LaneId
const CONTROL_LANE = { kind: 'control' } as const satisfies LaneId
const DEMAND_LANE = { kind: 'demand' } as const satisfies LaneId

function decodeRoomText(value: Uint8Array): string {
  return roomTextDecoder.decode(value)
}

function encodeRoomRecord(value: unknown): Uint8Array {
  return roomTextEncoder.encode(stringify(value))
}

/** A demand page is plain data only servers read, so it skips the room record encoding. */
function encodeDemandPage(page: RoomDemandPage): Uint8Array {
  return roomTextEncoder.encode(JSON.stringify(page))
}

/** A demand page as `encodeDemandPage()` wrote it, each want built as its entry's variant. */
function decodeDemandPage(serialized: string): RoomDemandPage {
  const page = fieldsOf(JSON.parse(serialized), 'A demand page')
  assert(page.__r === 'demand', 'A demand page of another kind')
  const { wants } = page
  assert(Array.isArray(wants), "A demand page's wants are not a list")
  return {
    __r: 'demand',
    instance: textOf(page.instance, "A demand page's instance"),
    page: numberOf(page.page, "A demand page's number"),
    wants: wants.map(demandWantOf),
  }
}

function demandWantOf(value: unknown): RoomDemandPage['wants'][number] {
  const want = fieldsOf(value, 'A demand want')
  const { again, on } = want
  if (again !== undefined) {
    assert(typeof again === 'boolean', 'A demand want asks again by no boolean')
    return { again }
  }
  assert(typeof on === 'boolean', 'A demand want is neither on nor off')
  return demandEntryOf(on, want)
}

/** The want of the `DemandEntry` variant the entry's keys make: a page's who, a member's track excepted from its
 *  track's lease, a track's lease, or a member's. */
function demandEntryOf(on: boolean, entry: Record<string | symbol, unknown>): { on: boolean } & DemandEntry {
  const { page, member, track, except } = entry
  if (page !== undefined) {
    const who: { on: boolean } & WhoEntry = { on, page: textOf(page, "A who entry's page") }
    const by = optionalText(entry.by, "A who entry's participant")
    if (by !== undefined) who.by = by
    const whose = optionalText(member, "A who entry's member")
    if (whose !== undefined) who.member = whose
    const which = optionalText(track, "A who entry's track")
    if (which !== undefined) who.track = which
    return who
  }
  if (except === true)
    return { on, track: textOf(track, 'A demand track'), member: textOf(member, 'A demand member'), except }
  if (except !== undefined) {
    const members = textListOf(except, "A track lease's exceptions")
    const leased = textOf(track, 'A demand track')
    return entry.byMember === true
      ? { on, track: leased, except: members, byMember: true }
      : { on, track: leased, except: members }
  }
  const of = textOf(member, 'A demand member')
  if (entry.byMember === true) return { on, member: of, byMember: true }
  return track === undefined ? { on, member: of } : { on, member: of, track: textOf(track, 'A demand track') }
}

/** An event of a room's control lane. */
function decodeCtrlEnvelope(serialized: string): RoomCtrlEnvelope {
  return ctrlEnvelopeOf(parse(serialized))
}

/** A message of a room's text lane: a member's, stamped by the instance it sent through, or the server's. */
function decodeDataEnvelope(serialized: string): RoomDataEnvelope {
  const fields = fieldsOf(parse(serialized), 'A text message')
  assert(fields.__r === 'data', 'A text message of another kind')
  const from = textOf(fields.from, "A message's sender")
  const { data, fromMeta } = fields
  if (fromMeta === undefined) return { __r: 'data', from, data }
  const envelope: RoomDataEnvelope = {
    __r: 'data',
    from,
    data,
    fromMeta: metaOf(fromMeta, "A message's sender meta"),
    fromMetaSeq: numberOf(fields.fromMetaSeq, "A message's sender meta seq"),
  }
  const fromIdentity = optionalText(fields.fromIdentity, "A message's sender identity")
  if (fromIdentity !== undefined) envelope.fromIdentity = fromIdentity
  if (fields.fromDisconnected === true) envelope.fromDisconnected = true
  return envelope
}

/** A frame of a member's inbox lane: a DM to it, or an ack DM's reply to one it sent. */
function decodeInboxEnvelope(serialized: string): RoomDmEnvelope | RoomDmAckEnvelope {
  const fields = fieldsOf(parse(serialized), 'An inbox frame')
  if (fields.__r === 'dm-ack') return dmAckEnvelopeOf(fields)
  assert(fields.__r === 'dm', 'An inbox frame of another kind')
  return dmEnvelopeOf(fields)
}

/** An ack DM's reply, on its sender's inbox lane or stored until the sender settles it. */
function dmAckEnvelopeOf(fields: Record<string | symbol, unknown>): RoomDmAckEnvelope {
  const to = textOf(fields.to, "A DM reply's recipient")
  const ackId = textOf(fields.ackId, "A DM reply's ack id")
  if (fields.ok === true) return { __r: 'dm-ack', to, ackId, ok: true, value: fields.value }
  return { __r: 'dm-ack', to, ackId, ...roomFailureOf(fields) }
}

/** A record `encodeRoomRecord()` wrote, its shape still to be read. */
function decodeRoomRecord(bytes: Uint8Array): unknown {
  return parse(decodeRoomText(bytes))
}

/** A DM's reply as `storeDmReply()` keeps it: its `dm-ack` frame. */
function dmReplyOf(bytes: Uint8Array): RoomDmAckEnvelope {
  const fields = fieldsOf(decodeRoomRecord(bytes), "A DM's stored reply")
  assert(fields.__r === 'dm-ack', "A DM's stored reply of another kind")
  return dmAckEnvelopeOf(fields)
}

/** A departed member's cause, kept while its leave settles. */
function departureCauseOf(bytes: Uint8Array): WireLeaveCause {
  return leaveCauseOf(fieldsOf(decodeRoomRecord(bytes), "A departure's cause"))
}

/** A member cell's record (`RoomMemberRecord`). */
function memberRecordOf(bytes: Uint8Array): RoomMemberRecord {
  const fields = fieldsOf(decodeRoomRecord(bytes), 'A member record')
  const record: RoomMemberRecord = {
    joinedAt: numberOf(fields.joinedAt, "A member's join time"),
    session: textOf(fields.session, "A member's lease"),
    metaSeq: numberOf(fields.metaSeq, "A member's meta seq"),
    metaGen: numberOf(fields.metaGen, "A member's meta generation"),
  }
  const identity = optionalText(fields.identity, "A member's identity")
  if (identity !== undefined) record.identity = identity
  if (fields.hidden !== undefined) {
    assert(typeof fields.hidden === 'boolean', "A member's hidden flag is not a boolean")
    record.hidden = fields.hidden
  }
  if (fields.disconnected === true) record.disconnected = true
  return record
}

/** The bytes of a commit's payload that its publisher published, serialized as `publish()` serializes a message: a
 *  message's data, or the whole payload of any other commit. */
function publishedBytes(lane: LaneId, payload: Uint8Array): number {
  if (lane.kind === 'binary') {
    const frame = decodeBinaryFrame(payload)
    assert(frame !== null)
    return frame.payload.byteLength
  }
  if (lane.kind !== 'semantic') return payload.byteLength
  const record = fieldsOf(decodeRoomRecord(payload), 'A retained record')
  return record.__r === 'data' ? utf8ByteLength(stringify(record.data)) : payload.byteLength
}

function configFromHead(head: RoomHead): RoomConfigRecord {
  const fields = fieldsOf(decodeRoomRecord(head.config), "A room's config")
  const { hold } = fields
  let holds: RoomConfigRecord['hold'] = null
  if (hold !== null) {
    const timeouts = fieldsOf(hold, "A room's hold")
    holds = {
      emptyTimeout: numberOf(timeouts.emptyTimeout, "A room's empty timeout"),
      departureTimeout: numberOf(timeouts.departureTimeout, "A room's departure timeout"),
    }
  }
  const config: RoomConfigRecord = {
    meta: metaOf(fields.meta, "A room's meta"),
    inc: textOf(fields.inc, "A room's inc"),
    hold: holds,
  }
  assert(head.currentInc === null || config.inc === head.currentInc)
  return config
}

/** The config of an open head, and only of `inc` when given. */
function openConfig(current: RoomHead | null, inc?: string): RoomConfigRecord | null {
  if (current?.state !== 'open' || (inc !== undefined && current.currentInc !== inc)) return null
  return configFromHead(current)
}

/** The config of `incarnation`, read from the head now; null once the room closed or a later incarnation replaced it. */
async function readOpenConfig(store: RoomStore, { id, inc }: RoomIncarnation): Promise<RoomConfigRecord | null> {
  return openConfig(await store.reads.readHead(id), inc)
}

/** An accepted commit, with why its handoff was lost if it was an inbox's. */
type LaneAccepted = CommitAccepted & { lost?: string }

/** An accepted commit; a member whose cell the commit required and that left; or the incarnation ended. */
type LaneCommit = LaneAccepted | { kind: 'gone'; member: string } | StaleIncarnation

/** Commits `payload` on `target`'s lane. An accepted inbox or control commit waits for its handoff up to the horizon: a
 *  server applies its own joins and leaves as it commits them, so a join's frame must not reach it after the member's
 *  leave. A lost inbox handoff is the sender's to hear: the message is gone. A control commit's state reaches every
 *  view by its reads. */
async function commitLane(
  store: RoomStore,
  target: CommitTarget,
  payload: Uint8Array,
  opts?: CommitOptions,
): Promise<LaneCommit> {
  const result = await store.commitLane(target, payload, opts)
  // Required cells are member records, so a missing one names the member that left.
  if (result.kind === 'stale-cell') return { kind: 'gone', member: memberIdOfCellKey(result.key) }
  if (result.kind === 'stale-incarnation' || result.delivery === undefined) return result
  const lost = await handoffLost(target.roomId, result.delivery)
  return lost !== null && target.lane.kind === 'inbox' ? { ...result, lost } : result
}

/** A write a call would make, as its size goes: its lane, its payload, and what the write keeps of it. */
type SizedCommit = { lane: LaneId; payload: Uint8Array; kept: CommitKept }

/** The commits a call would make to `members`, as their size goes. */
type CommitsOf = { commits(members: readonly MemberSnapshot[]): readonly SizedCommit[] }

/** A call's check that its commits fit, for a lookup to run on the members it read before it writes: each commit
 *  `call.commits()` gives for them must be committable, or `check()` throws the refusal the commit would meet. */
class CommitFitsBeforeWriting {
  constructor(
    private readonly store: RoomStore,
    private readonly call: CommitsOf,
  ) {}

  check(members: readonly MemberSnapshot[]): void {
    for (const { lane, payload, kept } of this.call.commits(members)) this.store.assertCommittable(lane, payload, kept)
  }
}

function commitFitsBeforeWriting(store: RoomStore, call: CommitsOf): CommitFitsBeforeWriting {
  return new CommitFitsBeforeWriting(store, call)
}

/** The accepted commit; a member that left or an ended incarnation throws. */
function acceptedOrThrow(roomId: string, commit: LaneCommit): LaneAccepted {
  if (commit.kind === 'gone') throw participantGoneError(commit.member)
  if (commit.kind === 'stale-incarnation') throw roomClosedError(roomId)
  return commit
}

function controlLane(roomId: string, inc: string): CommitTarget {
  return { roomId, inc, lane: CONTROL_LANE }
}

function inboxOf(roomId: string, inc: string, member: string): CommitTarget {
  return { roomId, inc, lane: { kind: 'inbox', member } }
}

/** Why `delivery` was lost, reported, or null once handed off. Delivery is at-most-once: a handoff that rejects or never
 *  settles is lost and never retried, and no bug (an authority a deploy evicts rejects it). */
async function handoffLost(roomId: string, delivery: Promise<void>): Promise<string | null> {
  const lost = await lostReason(delivery)
  if (lost !== null) reportDeliveryLost(roomId, lost)
  return lost
}

/** Null once `delivery` was handed off, else why it was lost: a handoff unconfirmed within the horizon is. */
function lostReason(delivery: Promise<void>): Promise<string | null> {
  return raceTimeout(rejectionOf(delivery), ROOM_HORIZON_MS, () => 'unconfirmed within the horizon')
}

async function rejectionOf(delivery: Promise<void>): Promise<string | null> {
  try {
    await delivery
    return null
  } catch (reason) {
    return String(reason)
  }
}

function withinRoomHorizon<T>(promise: Promise<T>): Promise<T> {
  return raceTimeout(promise, ROOM_HORIZON_MS, () => {
    throw new RoomError('BACKEND_TIMEOUT', 'Room subscription recovery horizon expired')
  })
}
