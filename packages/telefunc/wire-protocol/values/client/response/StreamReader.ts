export { StreamReader }

import { BaseStreamReader } from './BaseStreamReader.js'
import { splitOwned } from '../../../wire/envelope.js'
import { throwAbortError } from '../../../../client/remoteTelefunctionCall/errors.js'

const EMPTY = new Uint8Array(0)

/** Buffered reader for the inline binary HTTP response body (`'binary-inline'` transport).
 *
 *  readExact: read N bytes (low-level byte I/O with buffering).
 *  readU32 + readNextFrame: inherited from BaseStreamReader. */
class StreamReader extends BaseStreamReader {
  private buffer: Uint8Array<ArrayBuffer> = EMPTY

  async readExact(n: number): Promise<Uint8Array<ArrayBuffer>> {
    let parts: Uint8Array<ArrayBuffer>[] | undefined
    let length = this.buffer.length
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
        if (this.cancelled) return EMPTY
        throw readError ?? new Error('Connection lost — the server closed the stream before all data was received.')
      }
      parts ??= this.buffer.length > 0 ? [this.buffer] : []
      parts.push(read.value)
      length += read.value.length
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
