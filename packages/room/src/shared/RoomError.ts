export { RoomError, isRoomError, isRoomErrorCode }
export type { RoomErrorCode }

/** What a `RoomError` is about, for an app to branch on; its message is for people and may change. */
type RoomErrorCode = (typeof ROOM_ERROR_CODES)[number]
const ROOM_ERROR_CODES = [
  'ROOM_NOT_FOUND',
  'ROOM_EXISTS',
  'ROOM_CLOSING',
  'ROOM_CLOSED',
  'JOIN_NOT_GUARDED',
  'NOT_A_PARTICIPANT',
  'PARTICIPANT_NOT_FOUND',
  'PARTICIPANT_LEFT',
  'LIMIT_REACHED',
  'MESSAGE_LOST',
  'BACKEND_TIMEOUT',
  'MALFORMED',
  'INTERNAL',
  'DM_NO_LISTENER',
  'DM_INBOX_OVERFLOW',
  'DM_TIMEOUT',
  'DM_TWO_REPLIES',
  'DM_HANDLER_THREW',
  'DM_RECIPIENT_WENT_AWAY',
] as const

function isRoomErrorCode(thing: unknown): thing is RoomErrorCode {
  return ROOM_ERROR_CODES.some((code) => code === thing)
}

const roomErrorBrand = Symbol.for('telefunc.RoomError')

/** An expected caller-facing rejection; the global brand survives duplicate module graphs, `instanceof` included. */
class RoomError extends Error {
  readonly [roomErrorBrand] = true as const
  readonly code: RoomErrorCode

  constructor(code: RoomErrorCode, message: string) {
    super(message)
    this.name = 'RoomError'
    this.code = code
    Object.setPrototypeOf(this, new.target.prototype)
  }

  static [Symbol.hasInstance](thing: unknown): boolean {
    return isRoomError(thing)
  }
}

function isRoomError(thing: unknown): thing is RoomError {
  return typeof thing === 'object' && thing !== null && roomErrorBrand in thing
}
