export {
  CONTROL_LANE,
  SEMANTIC_LANE,
  commitRoomLane,
  commitRoomLaneOrThrow,
  openConfig,
  configFromHead,
  decodeRoomRecord,
  decodeRoomText,
  encodeRoomConfig,
  encodeRoomRecord,
  encodeRoomText,
  publishCtrl,
  staleCommitError,
  withinRoomHorizon,
}

import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { getRoomBackend } from '../../backend/install.js'
import type { CommitAccepted, LaneId, RoomHead, StaleCommit } from '../../backend/room/contract.js'
import type { RoomConfigRecord, RoomCtrlEnvelope } from '../protocol.js'
import { RoomError, participantGoneError, roomClosedError } from '../errors.js'
import { assert } from '../../../utils/assert.js'
import { ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS } from '../constants.js'
import { reportRoomError } from './errors.js'
import { memberIdOfCellKey } from './membership.js'

const roomTextEncoder = new TextEncoder()
const roomTextDecoder = new TextDecoder()
const SEMANTIC_LANE = { kind: 'semantic' } as const satisfies LaneId
const CONTROL_LANE = { kind: 'control' } as const satisfies LaneId

function encodeRoomText(value: string): Uint8Array {
  return roomTextEncoder.encode(value)
}

function decodeRoomText(value: Uint8Array): string {
  return roomTextDecoder.decode(value)
}

function encodeRoomRecord(value: unknown): Uint8Array {
  return encodeRoomText(stringify(value))
}

function decodeRoomRecord<T>(bytes: Uint8Array): T {
  return parse(decodeRoomText(bytes)) as T
}

function encodeRoomConfig(config: RoomConfigRecord): Uint8Array {
  return encodeRoomRecord(config)
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
  opts?: { retain?: boolean; closingLease?: string; requiredCellKeys?: string[] },
): Promise<CommitAccepted | StaleCommit> {
  const result = await getRoomBackend().commitLane(id, inc, lane, payload, opts)
  if ('stale' in result) return result
  // Delivery is at-most-once: a handoff lost with its fence (e.g. a partition) must not hang the caller.
  const delivered = raceTimeout(
    result.delivery.then(() => true),
    ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS,
    () => false,
  )
  if (!(await delivered))
    reportRoomError(new Error(`Room delivery unconfirmed after ${ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS} ms: ${id}`))
  return result
}

async function commitRoomLaneOrThrow(
  id: string,
  inc: string,
  lane: LaneId,
  payload: Uint8Array,
  opts?: { retain?: boolean; requiredCellKeys?: string[] },
): Promise<CommitAccepted> {
  const result = await commitRoomLane(id, inc, lane, payload, opts)
  if ('stale' in result) throw staleCommitError(id, result)
  return result
}

/** Settles like `promise`, or like `onTimeout()` once `ms` pass first; a spent budget times out at once. */
function raceTimeout<T>(promise: Promise<T>, ms: number, onTimeout: () => T): Promise<T> {
  if (ms <= 0) return Promise.resolve().then(onTimeout)
  let timer!: ReturnType<typeof setTimeout>
  const timeout = new Promise<T>((resolve) => {
    timer = unrefTimer(setTimeout(() => resolve(Promise.resolve().then(onTimeout)), ms))
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer))
}

function withinRoomHorizon<T>(promise: Promise<T>, ms: number): Promise<T> {
  return raceTimeout(promise, ms, () => {
    throw new RoomError('Room subscription recovery horizon expired')
  })
}

async function publishCtrl(roomId: string, inc: string, event: RoomCtrlEnvelope): Promise<void> {
  await commitRoomLaneOrThrow(roomId, inc, CONTROL_LANE, encodeRoomRecord(event))
}

/** Required cells are member records, so a missing one names the member that left. */
function staleCommitError(roomId: string, stale: StaleCommit): RoomError {
  return stale.stale === 'cell' ? participantGoneError(memberIdOfCellKey(stale.key)) : roomClosedError(roomId)
}
