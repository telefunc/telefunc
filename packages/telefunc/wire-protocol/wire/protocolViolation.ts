export { ProtocolViolationError, assertProtocol, parsePeerText }

import { parse } from '@brillout/json-serializer/parse'

import { markErrorClass } from '../../shared/markErrorClass.js'

/** Raised for every malformed-bytes path. Never `assert`: that one tells the user they found a
 *  telefunc bug, and these bytes are the peer's. */
class ProtocolViolationError extends Error {
  constructor(
    debugInfo?: string,
    readonly target?: unknown,
  ) {
    super(debugInfo)
  }
}

markErrorClass(ProtocolViolationError, 'ProtocolViolationError')

/** `assert` for peer input, `debugInfo` and all. `target` names the wire to terminate when it
 *  isn't the sender. */
function assertProtocol(condition: unknown, debugInfo: string, target?: unknown): asserts condition {
  if (!condition) throw new ProtocolViolationError(debugInfo, target)
}

/** Peer-authored JSON: a parse failure is the peer's, so it surfaces as a protocol violation. */
function parsePeerText(text: string): unknown {
  try {
    return parse(text)
  } catch {
    throw new ProtocolViolationError('peer payload is not parsable')
  }
}
