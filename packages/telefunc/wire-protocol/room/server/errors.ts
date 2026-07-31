export { reportRoomError, roomAckError }

import { stringify } from '@brillout/json-serializer/stringify'
import { handleTelefunctionBug } from '../../../node/server/runTelefunc/validateTelefunctionError.js'
import { ACK_STATUS, type AckResultStatus } from '../../shared-ws.js'
import { classifyTelefuncError } from '../../error-classification.js'
import { isRoomError, ROOM_BUG_MESSAGE } from '../errors.js'

function roomAckError(err: unknown, report: (err: unknown) => void): { text: string; status: AckResultStatus } {
  const classified = classifyTelefuncError(err, isRoomError)
  if (classified.kind === 'abort') return { text: stringify(classified.error.abortValue), status: ACK_STATUS.ABORT }
  if (classified.kind === 'expected') return { text: classified.error.message, status: ACK_STATUS.ERROR }
  if (classified.kind === 'shield') return { text: classified.error.message, status: ACK_STATUS.SHIELD_ERROR }
  report(err)
  return { text: ROOM_BUG_MESSAGE, status: ACK_STATUS.ERROR }
}

function reportRoomError(err: unknown): void {
  handleTelefunctionBug(err instanceof Error ? err : new Error(String(err)))
}
