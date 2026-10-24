export {
  RoomError,
  isRoomError,
  roomClosedError,
  participantGoneError,
  participantLeftError,
  toRoomFailure,
  ROOM_BUG_MESSAGE,
  CLIENT_ROOM_BUG_MESSAGE,
  roomFailureError,
  toRoomResult,
  roomResultValue,
  DM_FAILURE,
}

import { createAbortError } from '../../shared/Abort.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../shared/constants.js'
import { ChannelOverflowError } from '../channel/channelErrors.js'
import { classifyTelefuncError } from '../channel/errorClassification.js'
import type { DmReply, RoomFailure, RoomResult } from './protocol.js'
import { ShieldValidationError } from '../../shared/ShieldValidationError.js'

// Error contract: Abort carries its value, RoomError carries a safe message, and bugs are reported
// on the throwing side and hidden from the caller.
const roomErrorBrand = Symbol.for('telefunc.RoomError')

/** An expected caller-facing rejection; the global brand survives duplicate module graphs. */
class RoomError extends Error {
  readonly [roomErrorBrand] = true as const

  constructor(message: string) {
    super(message)
    this.name = 'RoomError'
    Object.setPrototypeOf(this, new.target.prototype)
  }
}

function isRoomError(thing: unknown): thing is RoomError {
  return typeof thing === 'object' && thing !== null && roomErrorBrand in thing
}

function roomClosedError(roomId: string): RoomError {
  return new RoomError(`Room is closed: ${roomId}`)
}

function participantGoneError(memberId: string): RoomError {
  return new RoomError(`Participant not found (left?): ${memberId}`)
}

const PARTICIPANT_LEFT = 'Participant left the room'

function participantLeftError(): RoomError {
  return new RoomError(PARTICIPANT_LEFT)
}

const ROOM_BUG_MESSAGE = `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`
const CLIENT_ROOM_BUG_MESSAGE = "Internal client room error — see the recipient's client logs"

/** An error as an ack DM's reply carries it: an Abort with its value, a RoomError's or a shield's message, a bug
 *  reported and hidden. */
function toRoomFailure(err: unknown, report: (err: unknown) => void, bugMessage: string): RoomFailure {
  const classified = classifyTelefuncError(err, isRoomError)
  if (classified.kind === 'abort') return { ok: false, abort: true, abortValue: classified.error.abortValue }
  if (classified.kind !== 'bug') return { ok: false, err: classified.error.message }
  report(err)
  return { ok: false, err: bugMessage }
}

function toRoomResult(work: Promise<unknown>, report: (err: unknown) => void): Promise<RoomResult> {
  return work.then(
    (value) => ({ ok: true, value }),
    (err: unknown): RoomResult => {
      const classified = classifyTelefuncError(err, isRoomError)
      if (classified.kind === 'shield') return { ok: false, shield: classified.error.message }
      if (err instanceof ChannelOverflowError) return { ok: false, overflow: err.message }
      return toRoomFailure(err, report, ROOM_BUG_MESSAGE)
    },
  )
}

async function roomResultValue<T = unknown>(answer: Promise<unknown>): Promise<T> {
  const result = (await answer) as RoomResult
  if (result.ok) return result.value as T
  if ('abort' in result) throw createAbortError(result.abortValue)
  if ('shield' in result) throw new ShieldValidationError(result.shield)
  if ('overflow' in result) throw new ChannelOverflowError(result.overflow)
  throw new Error(result.err)
}

function roomFailureError(res: RoomFailure): Error {
  if ('abort' in res) return createAbortError(res.abortValue)
  return new RoomError(res.err)
}

/** The replies an `{ ack: true }` sender gets when no handler of the recipient answered. */
const DM_FAILURE = {
  left: { ok: false, err: PARTICIPANT_LEFT },
  noListener: { ok: false, err: 'No participant inbox listener is attached' },
  overflow: { ok: false, err: 'Inbox overflowed before the message was handled' },
  timeout: { ok: false, err: 'send({ ack: true }) timed out: the recipient never handled the message' },
  roomClosed: { ok: false, err: 'Room is closed' },
} as const satisfies Record<string, DmReply>
