export { SSEStreamReader }

import { BaseStreamReader } from './BaseStreamReader.js'
import { SSEEvents } from '../SSEEvents.js'
import { concatAll } from '../../frame.js'
import { throwAbortError } from '../../../client/remoteTelefunctionCall/errors.js'

const EMPTY = new Uint8Array(0)

/** SSE transport reader — decodes `data: <base64url>\n\n` events back to
 *  binary frames.
 *
 *  `readExact` is the only method implemented here. SSE parser state
 *  (`sse`) lives as an instance field,
 *  persisted across calls. `reader.read()` is called inline — only when more
 *  bytes are needed — preserving the full pull chain through
 *  `FrameDemuxer.ensureReading` to the server generator. No separate pump. */
class SSEStreamReader extends BaseStreamReader {
  private reader: ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>
  private binary: Uint8Array<ArrayBuffer> = EMPTY
  /** Events decoded since `binary`, joined once `readExact` has its bytes. */
  private events: Uint8Array<ArrayBuffer>[] = []
  private eventBytes = 0
  private sse = new SSEEvents()

  constructor(
    reader: ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>,
    callContext: {
      telefunctionName: string
      telefuncFilePath: string
      abortController: AbortController
    },
  ) {
    super(callContext)
    this.reader = reader
    callContext.abortController.signal.addEventListener('abort', () => reader.cancel().catch(() => {}), { once: true })
  }

  cancel(): void {
    this.cancelled = true
    // An errored body rejects its cancel; that error already reached the reads.
    this.reader.cancel().catch(() => {})
  }

  async readExact(n: number): Promise<Uint8Array<ArrayBuffer>> {
    while (this.binary.length + this.eventBytes < n) {
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
        throw readError ?? new Error('Connection lost — server closed the SSE stream before all data was received.')
      }
      this.sse.push(value!, this.onEvent)
    }
    if (this.events.length > 0) {
      const parts = this.binary.length > 0 ? [this.binary, ...this.events] : this.events
      this.binary = parts.length === 1 ? parts[0]! : concatAll(parts)
      this.events = []
      this.eventBytes = 0
    }
    const result = this.binary.subarray(0, n)
    this.binary = n < this.binary.length ? this.binary.subarray(n) : EMPTY
    return result
  }

  private onEvent = (event: Uint8Array<ArrayBuffer>) => {
    this.events.push(event)
    this.eventBytes += event.length
  }
}
