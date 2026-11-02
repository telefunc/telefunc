export {
  CONTROL_LANE,
  DEMAND_LANE,
  SEMANTIC_LANE,
  acceptedOrThrow,
  commitLane,
  controlLane,
  inboxOf,
  openConfig,
  configFromHead,
  decodeDemandPage,
  decodeLaneEnvelope,
  decodeRoomRecord,
  decodeRoomText,
  encodeDemandPage,
  encodeRoomRecord,
  withinRoomHorizon,
}

import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { raceTimeout } from '../../utils/raceTimeout.js'
import { getRoomPlane } from '../../backend/plane.js'
import type {
  CommitAccepted,
  CommitOptions,
  CommitTarget,
  LaneId,
  RoomHead,
  StaleIncarnation,
} from '../../backend/contract.js'
import { hasRoomTag } from '../../shared/protocol.js'
import type { RoomConfigRecord, RoomDemandPage } from '../protocol.js'
import { RoomError, participantGoneError, roomClosedError } from '../../shared/errors.js'
import { assert } from '../../utils/assert.js'
import { ROOM_HORIZON_MS } from '../constants.js'
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

function decodeDemandPage(serialized: string): RoomDemandPage {
  const page = JSON.parse(serialized) as RoomDemandPage
  assert(page.__r === 'demand')
  return page
}

/** Lanes carry only Room's own encodings. */
function decodeLaneEnvelope(serialized: string): { __r: string } {
  const envelope: unknown = parse(serialized)
  assert(hasRoomTag(envelope))
  return envelope
}

function decodeRoomRecord<T>(bytes: Uint8Array): T {
  return parse(decodeRoomText(bytes)) as T
}

function configFromHead(head: RoomHead): RoomConfigRecord {
  const config = decodeRoomRecord<RoomConfigRecord>(head.config)
  assert(head.currentInc === null || config.inc === head.currentInc)
  return config
}

/** The config of an open head, and only of `inc` when given. */
function openConfig(current: RoomHead | null, inc?: string): RoomConfigRecord | null {
  if (current?.state !== 'open' || (inc !== undefined && current.currentInc !== inc)) return null
  return configFromHead(current)
}

/** An accepted commit; a member whose cell the commit required and that left; or the incarnation ended. */
type LaneCommit = CommitAccepted | { kind: 'gone'; member: string } | StaleIncarnation

/** Commits `payload` on `target`'s lane. An accepted inbox or control commit waits for its handoff up to the horizon: a
 *  server applies its own joins and leaves as it commits them, so a join's frame must not reach it after the member's
 *  leave. */
async function commitLane(target: CommitTarget, payload: Uint8Array, opts?: CommitOptions): Promise<LaneCommit> {
  const result = await getRoomPlane().commitLane(target, payload, opts)
  // Required cells are member records, so a missing one names the member that left.
  if (result.kind === 'stale-cell') return { kind: 'gone', member: memberIdOfCellKey(result.key) }
  if (result.kind === 'stale-incarnation') return result
  if (result.delivery !== undefined) await handedOff(target.roomId, result)
  return result
}

/** The accepted commit; a member that left or an ended incarnation throws. */
function acceptedOrThrow(roomId: string, commit: LaneCommit): CommitAccepted {
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

/** Delivery is at-most-once: a handoff that rejects or never settles is lost, not the committer's failure, and no bug
 *  (an authority a deploy evicts rejects it), so it is logged, as a Broadcast delivery that can't reach a DO is. */
async function handedOff(roomId: string, commit: CommitAccepted): Promise<void> {
  assert(commit.delivery !== undefined)
  const lost = await raceTimeout(
    commit.delivery.then(
      () => null,
      (reason: unknown) => String(reason),
    ),
    ROOM_HORIZON_MS,
    () => 'unconfirmed within the horizon',
  )
  if (lost !== null) console.error(`Room delivery handoff of '${roomId}' lost: ${lost}`)
}

function withinRoomHorizon<T>(promise: Promise<T>): Promise<T> {
  return raceTimeout(promise, ROOM_HORIZON_MS, () => {
    throw new RoomError('Room subscription recovery horizon expired')
  })
}
