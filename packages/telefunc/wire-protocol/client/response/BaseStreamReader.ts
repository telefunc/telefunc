export { BaseStreamReader }

import { parse } from '@brillout/json-serializer/parse'
import { assert } from '../../../utils/assert.js'
import { isObject } from '../../../utils/isObject.js'
import { decodeU32, decodeIndexedFrame } from '../../frame.js'
import { STREAMING_ERROR_FRAME_MARKER, STREAMING_ERROR_TYPE } from '../../constants.js'
import { throwAbortError, throwBugError } from '../../../client/remoteTelefunctionCall/errors.js'

type CallContext = {
  telefunctionName: string
  telefuncFilePath: string
  abortController: AbortController
}

/** Abstract base for all transport readers.
 *
 *  `readU32` and `readNextFrame` are pure wire-protocol logic — they only call
 *  `readExact`. Each transport implements only `readExact` (the byte source). */
abstract class BaseStreamReader {
  cancelled = false
  protected callContext: CallContext

  constructor(callContext: CallContext) {
    this.callContext = callContext
  }

  /** Cancel reading — sets cancelled flag and cleans up the transport resource. */
  abstract cancel(): void

  /** Implemented by each transport — pull exactly n bytes from the wire source. */
  abstract readExact(n: number): Promise<Uint8Array<ArrayBuffer>>

  async readU32(): Promise<number> {
    const buf = await this.readExact(4) // sizeof uint32
    return this.cancelled ? 0 : decodeU32(buf)
  }

  /** Read the next indexed frame from the wire.
   *  Returns { index, payload } or null on terminator. Throws on error frames. */
  async readNextFrame(): Promise<{ index: number; payload: Uint8Array<ArrayBuffer> } | null> {
    const len = await this.readU32()
    if (this.cancelled) return null
    if (len === 0) return null
    if (len === STREAMING_ERROR_FRAME_MARKER) {
      // Error frame: [ERROR_MARKER][u32 payload_len][payload_bytes]
      const errorLen = await this.readU32()
      const errorBytes = await this.readExact(errorLen)
      const errorPayload: unknown = parse(new TextDecoder().decode(errorBytes))
      assert(isObject(errorPayload) && 'type' in errorPayload)
      if (errorPayload.type === STREAMING_ERROR_TYPE.ABORT) {
        assert('abortValue' in errorPayload)
        throwAbortError(this.callContext.telefunctionName, this.callContext.telefuncFilePath, errorPayload.abortValue)
      }
      throwBugError()
    }
    const frameData = await this.readExact(len)
    if (this.cancelled) return null
    return decodeIndexedFrame(frameData)
  }
}
