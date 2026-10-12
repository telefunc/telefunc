export {
  CONTROL_LANE,
  DEMAND_LANE,
  SEMANTIC_LANE,
  commitRoomLane,
  commitRoomLaneOrThrow,
  commitToInbox,
  openConfig,
  configFromHead,
  decodeDemandPage,
  decodeLaneEnvelope,
  decodeRoomRecord,
  decodeRoomText,
  encodeDemandPage,
  encodeRoomRecord,
  publishCtrl,
  withinRoomHorizon,
}

import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { raceTimeout } from '../../../utils/raceTimeout.js'
import { getRoomBackend } from '../../backend/install.js'
import type { CommitAccepted, LaneId, RoomHead, StaleCommit, CommitOptions } from '../../backend/room/contract.js'
import {
  hasRoomTag,
  type RoomConfigRecord,
  type RoomCtrlEnvelope,
  type RoomDemandPage,
  type RoomDmAckEnvelope,
  type RoomDmEnvelope,
} from '../protocol.js'
import { RoomError, participantGoneError, roomClosedError } from '../errors.js'
import { assert } from '../../../utils/assert.js'
import { ROOM_HORIZON_MS } from '../constants.js'
import { reportRoomError } from './errors.js'
import { memberCellKey, memberIdOfCellKey } from './cells.js'

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

async function commitRoomLane(
  id: string,
  inc: string,
  lane: LaneId,
  payload: Uint8Array,
  opts?: CommitOptions,
): Promise<CommitAccepted | StaleCommit> {
  const result = await getRoomBackend().commitLane(id, inc, lane, payload, opts)
  if ('stale' in result) return result
  // Delivery is at-most-once: a handoff that rejects or never settles is lost, not the caller's failure.
  const delivered = raceTimeout(
    result.delivery.then(
      () => true,
      () => false,
    ),
    ROOM_HORIZON_MS,
    () => false,
  )
  if (!(await delivered))
    reportRoomError(new Error(`Room delivery handoff lost (rejected or unconfirmed within the horizon): ${id}`))
  return result
}

async function commitRoomLaneOrThrow(
  id: string,
  inc: string,
  lane: LaneId,
  payload: Uint8Array,
  opts?: Omit<CommitOptions, 'closingLease'>,
): Promise<CommitAccepted> {
  const result = await commitRoomLane(id, inc, lane, payload, opts)
  if ('stale' in result) throw staleCommitError(id, result)
  return result
}

/** Commits `envelope` to its recipient's inbox, requiring the recipient's member cell and each of `requiring`'s. A
 *  closed room throws; a required member that left is returned as `gone`, for the caller to throw or skip. */
async function commitToInbox(
  roomId: string,
  inc: string,
  envelope: RoomDmEnvelope | RoomDmAckEnvelope,
  requiring: string[] = [],
): Promise<CommitAccepted | { gone: string }> {
  const lane = { kind: 'inbox', member: envelope.to } as const
  const requiredCellKeys = [...requiring, envelope.to].map(memberCellKey)
  const result = await commitRoomLane(roomId, inc, lane, encodeRoomRecord(envelope), { requiredCellKeys })
  if (!('stale' in result)) return result
  if (result.stale === 'cell') return { gone: memberIdOfCellKey(result.key) }
  throw roomClosedError(roomId)
}

function withinRoomHorizon<T>(promise: Promise<T>): Promise<T> {
  return raceTimeout(promise, ROOM_HORIZON_MS, () => {
    throw new RoomError('Room subscription recovery horizon expired')
  })
}

async function publishCtrl(
  roomId: string,
  inc: string,
  event: RoomCtrlEnvelope,
  opts?: { requiredCellKeys: string[] },
): Promise<void> {
  await commitRoomLaneOrThrow(roomId, inc, CONTROL_LANE, encodeRoomRecord(event), opts)
}

/** Required cells are member records, so a missing one names the member that left. */
function staleCommitError(roomId: string, stale: StaleCommit): RoomError {
  return stale.stale === 'cell' ? participantGoneError(memberIdOfCellKey(stale.key)) : roomClosedError(roomId)
}
