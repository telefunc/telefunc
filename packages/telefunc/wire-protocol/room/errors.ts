export {
  RoomError,
  isRoomError,
  roomClosedError,
  participantGoneError,
  toRoomFailure,
  roomAckError,
  roomFailureError,
  ROOM_BUG_MESSAGE,
  DM_PARTICIPANT_LEFT,
}

import { createAbortError } from '../../shared/Abort.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../shared/constants.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { classifyTelefuncError } from '../error-classification.js'
import { ACK_STATUS, type AckResultStatus } from '../shared-ws.js'
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
// One classification, rendered for its two carriers.
/** The failure an ack DM's reply carries: it travels on the recipient's inbox lane, not as a channel ack. */
function toRoomFailure(err: unknown, report: (err: unknown) => void): RoomFailure {
  const classified = classifyTelefuncError(err, isRoomError)
  if (classified.kind === 'abort') return { ok: false, abort: true, abortValue: classified.error.abortValue }
  if (classified.kind !== 'bug') return { ok: false, err: classified.error.message }
  report(err)
  return { ok: false, err: ROOM_BUG_MESSAGE }
}
function roomAckError(err: unknown, report: (err: unknown) => void): { text: string; status: AckResultStatus } {
  const classified = classifyTelefuncError(err, isRoomError)
  if (classified.kind === 'abort') return { text: stringify(classified.error.abortValue), status: ACK_STATUS.ABORT }
  if (classified.kind === 'expected') return { text: classified.error.message, status: ACK_STATUS.ERROR }
  if (classified.kind === 'shield') return { text: classified.error.message, status: ACK_STATUS.SHIELD_ERROR }
  report(err)
  return { text: ROOM_BUG_MESSAGE, status: ACK_STATUS.ERROR }
}
function roomFailureError(res: RoomFailure): Error {
  if ('abort' in res) return createAbortError(res.abortValue)
  return new RoomError(res.err)
}

/** The reply an `{ ack: true }` sender gets when its recipient left, wherever the departure is noticed. */
const DM_PARTICIPANT_LEFT: DmReply = { ok: false, err: 'Participant left the room' }
