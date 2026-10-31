export { ProtocolViolationError, isProtocolViolationError, assertProtocol, parsePeerText }

import { parse } from '@brillout/json-serializer/parse'

const protocolViolationErrorBrand = Symbol.for('telefunc.room.ProtocolViolationError')

/** A page's malformed message. Never `assert`: that one tells the user they found a bug, and these bytes are the
 *  page's. */
class ProtocolViolationError extends Error {
  readonly [protocolViolationErrorBrand] = true as const
}

function isProtocolViolationError(thing: unknown): thing is ProtocolViolationError {
  return typeof thing === 'object' && thing !== null && protocolViolationErrorBrand in thing
}

/** `assert` for a page's input. */
function assertProtocol(condition: unknown, debugInfo: string): asserts condition {
  if (!condition) throw new ProtocolViolationError(debugInfo)
}

/** Page-authored JSON: a parse failure is the page's, so it surfaces as a protocol violation. */
function parsePeerText(text: string): unknown {
  try {
    return parse(text)
  } catch {
    throw new ProtocolViolationError('peer payload is not parsable')
  }
}
