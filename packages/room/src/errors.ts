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
  roomResultValue,
  DM_FAILURE,
}

import {
  Abort,
  ChannelOverflowError,
  ShieldValidationError,
  isChannelOverflowError,
  isShieldValidationError,
} from 'telefunc/__internal'
import type { RoomFailure, RoomResult } from './protocol.js'

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

const ROOM_BUG_MESSAGE = 'Internal Server Error — see server logs'
const CLIENT_ROOM_BUG_MESSAGE = "Internal client room error — see the recipient's client logs"

/** An error as a Room answer carries it: an Abort with its value, a shield's, an overflow's or a RoomError's message, a
 *  bug reported and hidden behind `bugMessage`. */
function toRoomFailure(err: unknown, report: (err: unknown) => void, bugMessage: string): RoomFailure {
  if (err instanceof Abort) return { ok: false, kind: 'abort', abortValue: err.abortValue }
  if (isRoomError(err)) return { ok: false, kind: 'error', message: err.message }
  if (isShieldValidationError(err)) return { ok: false, kind: 'shield', message: err.message }
  if (isChannelOverflowError(err)) return { ok: false, kind: 'overflow', message: err.message }
  report(err)
  return { ok: false, kind: 'error', message: bugMessage }
}

/** The error a caller's promise rejects with. */
function roomFailureError(failure: RoomFailure): Error {
  switch (failure.kind) {
    case 'abort':
      return Abort(failure.abortValue)
    case 'shield':
      return new ShieldValidationError(failure.message)
    case 'overflow':
      return new ChannelOverflowError(failure.message)
    case 'error':
      return new RoomError(failure.message)
  }
}

async function roomResultValue<T = unknown>(answer: Promise<unknown>): Promise<T> {
  const result = (await answer) as RoomResult
  if (!result.ok) throw roomFailureError(result)
  return result.value as T
}

/** The replies an `{ ack: true }` sender gets when no handler of the recipient answered. */
const DM_FAILURE = {
  left: { ok: false, kind: 'error', message: PARTICIPANT_LEFT },
  noListener: { ok: false, kind: 'error', message: 'No participant inbox listener is attached' },
  overflow: { ok: false, kind: 'error', message: 'Inbox overflowed before the message was handled' },
  timeout: {
    ok: false,
    kind: 'error',
    message: 'send({ ack: true }) timed out: the recipient never handled the message',
  },
  roomClosed: { ok: false, kind: 'error', message: 'Room is closed' },
} as const satisfies Record<string, RoomFailure>
