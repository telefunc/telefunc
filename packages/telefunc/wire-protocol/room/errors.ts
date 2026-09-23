export {
  RoomError,
  isRoomError,
  roomClosedError,
  participantGoneError,
  toRoomFailure,
  roomFailureError,
  ROOM_BUG_MESSAGE,
  DM_PARTICIPANT_LEFT,
}

import { createAbortError } from '../../shared/Abort.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../shared/constants.js'
import { classifyTelefuncError } from '../error-classification.js'
import type { DmReply, RoomFailure } from './protocol.js'

// Error contract: Abort carries its value, RoomError carries a safe message, and bugs are reported
// on the throwing side and hidden from the caller.
const roomErrorBrand = Symbol.for('telefunc.RoomError')
/** An expected caller-facing rejection; the global brand survives duplicate module graphs. */
class RoomError extends Error {
  readonly [roomErrorBrand] = true as const
  constructor(message: string) {
    super(message)
    this.name = 'RoomError'
    // Restore the prototype chain across the down-levelled `extends Error`.
    Object.setPrototypeOf(this, new.target.prototype)
  }
}
function isRoomError(thing: unknown): thing is RoomError {
  return thing instanceof RoomError || (typeof thing === 'object' && thing !== null && roomErrorBrand in thing)
}
function roomClosedError(roomId: string): RoomError {
  return new RoomError(`Room is closed: ${roomId}`)
}
function participantGoneError(memberId: string): RoomError {
  return new RoomError(`Participant not found (left?): ${memberId}`)
}
const ROOM_BUG_MESSAGE = `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`
/** The failure an ack DM's reply carries: it travels on the recipient's inbox lane, not as a channel ack. */
function toRoomFailure(err: unknown, report: (err: unknown) => void): RoomFailure {
  const classified = classifyTelefuncError(err, isRoomError)
  if (classified.kind === 'abort') return { ok: false, abort: true, abortValue: classified.error.abortValue }
  if (classified.kind === 'expected') return { ok: false, err: classified.error.message }
  report(err)
  return { ok: false, err: ROOM_BUG_MESSAGE }
}
function roomFailureError(res: RoomFailure): Error {
  if ('abort' in res) return createAbortError(res.abortValue)
  return new RoomError(res.err)
}

/** The `{ ack: true }` reply a sender gets when its recipient has already left — resolved (never left hanging) with one stable reason, wherever the departure is noticed (its stub, its held inbox). */
const DM_PARTICIPANT_LEFT: DmReply = { ok: false, err: 'Participant left the room' }
