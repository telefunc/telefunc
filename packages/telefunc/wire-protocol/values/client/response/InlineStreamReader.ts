export { InlineStreamReader }

import { parse } from '@brillout/json-serializer/parse'
import { assert } from '../../../../utils/assert.js'
import { isObject } from '../../../../utils/isObject.js'
import { decodeIndexedFrame, decodeU32, splitOwned, textDecoder } from '../../../wire/envelope.js'
import { STREAMING_ERROR_FRAME_MARKER, STREAMING_ERROR_TYPE } from '../../../constants.js'
import { throwAbortError, throwBugError } from '../../../../client/remoteTelefunctionCall/errors.js'
import { SseEvents } from '../../../wire/SseEvents.js'

const EMPTY = new Uint8Array(0)

type CallContext = {
  telefunctionName: string
  telefuncFilePath: string
  abortController: AbortController
}

/** Reads length-prefixed inline response frames on demand, decoding SSE events where the response uses SSE. */
class InlineStreamReader {
  private cancelled = false
  private buffer: Uint8Array<ArrayBuffer> = EMPTY
  private readonly sse: SseEvents | null

  constructor(
    private readonly reader: ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>,
    private readonly callContext: CallContext,
    isSse: boolean,
  ) {
    this.sse = isSse ? new SseEvents() : null
    // Cancel the reader on fetch abort so Chromium doesn't reject an unobserved body read.
    callContext.abortController.signal.addEventListener('abort', () => reader.cancel().catch(() => {}), { once: true })
  }

  cancel(): void {
    this.cancelled = true
    // An errored body rejects its cancel; that error already reached the reads.
    this.reader.cancel().catch(() => {})
  }

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
      const errorPayload: unknown = parse(textDecoder.decode(errorBytes))
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

  async readExact(n: number): Promise<Uint8Array<ArrayBuffer>> {
    let parts: Uint8Array<ArrayBuffer>[] | undefined
    let length = this.buffer.length
    while (length < n) {
      let done: boolean
      let value: Uint8Array<ArrayBuffer> | undefined
      let readError: unknown
      try {
        ;({ done, value } = await this.reader.read())
      } catch (err) {
        readError = err
        done = true
      }
      if (done) {
        if (this.callContext.abortController.signal.aborted) {
          throwAbortError(this.callContext.telefunctionName, this.callContext.telefuncFilePath, undefined)
        }
        if (this.cancelled) return EMPTY
        throw (
          readError ??
          new Error(
            this.sse === null
              ? 'Connection lost — the server closed the stream before all data was received.'
              : 'Connection lost — server closed the SSE stream before all data was received.',
          )
        )
      }
      const gathered = (parts ??= this.buffer.length > 0 ? [this.buffer] : [])
      if (this.sse === null) {
        gathered.push(value!)
        length += value!.length
      } else {
        this.sse.push(value!, (event) => {
          gathered.push(event)
          length += event.length
        })
      }
    }
    if (parts) {
      const [result, rest] = splitOwned(parts, n)
      this.buffer = rest
      return result
    }
    const result = this.buffer.subarray(0, n)
    this.buffer = n < this.buffer.length ? this.buffer.subarray(n) : EMPTY
    return result
  }
}
