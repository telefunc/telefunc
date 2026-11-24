export {
  RoomError,
  isRoomError,
  roomClosedError,
  participantGoneError,
  participantLeftError,
  messageLostError,
  toRoomFailure,
  ROOM_BUG_MESSAGE,
  CLIENT_ROOM_BUG_MESSAGE,
  roomFailureError,
  roomResultValue,
  publishAck,
  DM_FAILURE,
}

import { Abort, ChannelOverflowError, ShieldValidationError, isShieldValidationError } from 'telefunc/client'
import { isChannelOverflowError, isObject, type ChannelPublishAck } from 'telefunc/__internal'
import type { RoomFailure, RoomResult } from './protocol.js'
import { RoomError, isRoomError, isRoomErrorCode } from './RoomError.js'
import { assert } from '../utils/assert.js'

// Error contract: Abort carries its value, RoomError carries a code and a safe message, and bugs are reported on the
// throwing side and hidden from the caller.

function roomClosedError(roomId: string): RoomError {
  return new RoomError('ROOM_CLOSED', `Room is closed: ${roomId}`)
}

function participantGoneError(memberId: string): RoomError {
  return new RoomError('PARTICIPANT_NOT_FOUND', `Participant not found (left?): ${memberId}`)
}

const PARTICIPANT_LEFT = 'Participant left the room'

function participantLeftError(): RoomError {
  return new RoomError('PARTICIPANT_LEFT', PARTICIPANT_LEFT)
}

/** A message whose handoff to its recipient failed after it was committed; it isn't sent again. */
function messageLostError(): RoomError {
  return new RoomError(
    'MESSAGE_LOST',
    "Message may not be delivered: its handoff to the recipient failed, and it isn't sent again",
  )
}

const ROOM_BUG_MESSAGE = 'Internal Server Error — see server logs'
const CLIENT_ROOM_BUG_MESSAGE = "Internal client room error — see the recipient's client logs"

/** An error as a Room answer carries it: an Abort with its value, a shield's or an overflow's message, a RoomError's
 *  code and message, a bug reported and hidden behind `bugMessage`. */
function toRoomFailure(err: unknown, report: (err: unknown) => void, bugMessage: string): RoomFailure {
  if (err instanceof Abort) return { ok: false, kind: 'abort', abortValue: err.abortValue }
  if (isRoomError(err)) return { ok: false, kind: 'error', code: err.code, message: err.message }
  if (isShieldValidationError(err)) return { ok: false, kind: 'shield', message: err.message }
  if (isChannelOverflowError(err)) return { ok: false, kind: 'overflow', message: err.message }
  report(err)
  return { ok: false, kind: 'error', code: 'INTERNAL', message: bugMessage }
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
      return new RoomError(failure.code, failure.message)
  }
}

/** A Room answer's value, else the error its failure rejects with. */
async function roomResultValue(answer: Promise<unknown>): Promise<unknown> {
  const result = roomResult(await answer)
  if (!result.ok) throw roomFailureError(result)
  return result.value
}

/** The value of a Room answer to a publish: the place the publish took. */
function publishAck(value: unknown): ChannelPublishAck {
  assert(isObject(value), 'A Room publish was answered with no ack')
  const { key, seq, timestamp } = value
  assert(typeof key === 'string' && typeof seq === 'number' && typeof timestamp === 'number')
  return { key, seq, timestamp }
}

/** A Room answer as the server sends it (`toRoomFailure()`). */
function roomResult(answer: unknown): RoomResult {
  assert(isObject(answer), 'A Room answer is not an object')
  if (answer.ok === true) return { ok: true, value: answer.value }
  assert(answer.ok === false, 'A Room answer is neither a value nor a failure')
  const { kind, message, code } = answer
  if (kind === 'abort') return { ok: false, kind, abortValue: answer.abortValue }
  assert(typeof message === 'string', 'A Room failure has no message')
  if (kind === 'error') {
    assert(isRoomErrorCode(code), 'A Room failure has no error code')
    return { ok: false, kind, code, message }
  }
  assert(kind === 'shield' || kind === 'overflow', 'A Room failure is of no known kind')
  return { ok: false, kind, message }
}

/** The replies an `{ ack: true }` sender gets when no one handler of the recipient answered. */
const DM_FAILURE = {
  left: { ok: false, kind: 'error', code: 'PARTICIPANT_LEFT', message: PARTICIPANT_LEFT },
  noListener: {
    ok: false,
    kind: 'error',
    code: 'DM_NO_LISTENER',
    message: 'No participant inbox listener is attached',
  },
  overflow: {
    ok: false,
    kind: 'error',
    code: 'DM_INBOX_OVERFLOW',
    message: 'Inbox overflowed before the message was handled',
  },
  timeout: {
    ok: false,
    kind: 'error',
    code: 'DM_TIMEOUT',
    message: 'send({ ack: true }) timed out: the recipient never handled the message',
  },
  roomClosed: { ok: false, kind: 'error', code: 'ROOM_CLOSED', message: 'Room is closed' },
  twoReplies: {
    ok: false,
    kind: 'error',
    code: 'DM_TWO_REPLIES',
    message: 'Only one listen() callback may return a reply',
  },
  // A handler's own error stays where it ran, as a telefunction's stays on the server.
  handlerThrew: {
    ok: false,
    kind: 'error',
    code: 'DM_HANDLER_THREW',
    message: "The recipient's listen() callback threw: its error is logged where the recipient runs",
  },
  wentAway: {
    ok: false,
    kind: 'error',
    code: 'DM_RECIPIENT_WENT_AWAY',
    message: 'The recipient went away after it got the message, before it replied',
  },
} as const satisfies Record<string, RoomFailure>
