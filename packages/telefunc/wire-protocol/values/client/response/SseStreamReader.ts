export { SseStreamReader }

import { BaseStreamReader } from './BaseStreamReader.js'
import { SseEvents } from '../../../wire/SseEvents.js'
import { splitOwned } from '../../../wire/envelope.js'
import { throwAbortError } from '../../../../client/remoteTelefunctionCall/errors.js'

const EMPTY = new Uint8Array(0)

/** SSE transport reader — decodes `data: <base64url>\n\n` events back to
 *  binary frames.
 *
 *  `readExact` is the only method implemented here. SSE parser state
 *  (`sse`) lives as an instance field,
 *  persisted across calls. `reader.read()` is called inline — only when more
 *  bytes are needed — preserving the full pull chain through
 *  `FrameDemuxer.ensureReading` to the server generator. No separate pump. */
class SseStreamReader extends BaseStreamReader {
  private binary: Uint8Array<ArrayBuffer> = EMPTY
  private sse = new SseEvents()

  async readExact(n: number): Promise<Uint8Array<ArrayBuffer>> {
    let parts: Uint8Array<ArrayBuffer>[] | undefined
    let length = this.binary.length
    while (length < n) {
      let read: ReadableStreamReadResult<Uint8Array<ArrayBuffer>>
      let readError: unknown
      try {
        read = await this.reader.read()
      } catch (err) {
        readError = err
        read = { done: true, value: undefined }
      }
      if (read.done) {
        if (this.callContext.abortController.signal.aborted) {
          throwAbortError(this.callContext.telefunctionName, this.callContext.telefuncFilePath, undefined)
        }
        if (this.released) return EMPTY
        throw readError ?? new Error('Connection lost — server closed the SSE stream before all data was received.')
      }
      const gathered = (parts ??= this.binary.length > 0 ? [this.binary] : [])
      this.sse.push(read.value, (event) => {
        gathered.push(event)
        length += event.length
      })
    }
    if (parts) {
      const [result, rest] = splitOwned(parts, n)
      this.binary = rest
      return result
    }
    const result = this.binary.subarray(0, n)
    this.binary = n < this.binary.length ? this.binary.subarray(n) : EMPTY
    return result
  }
}
