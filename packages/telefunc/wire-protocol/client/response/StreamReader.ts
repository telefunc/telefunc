export { StreamReader }

import { BaseStreamReader } from './BaseStreamReader.js'
import { concat } from '../../frame.js'
import { throwCancelError } from '../../../client/remoteTelefunctionCall/errors.js'

const EMPTY = new Uint8Array(0)

/** Buffered reader for the HTTP response body stream (`'stream'` transport).
 *
 *  readExact: read N bytes (low-level byte I/O with buffering).
 *  readU32 + readNextFrame: inherited from BaseStreamReader. */
class StreamReader extends BaseStreamReader {
  private reader: ReadableStreamDefaultReader<Uint8Array>
  private buffer: Uint8Array = EMPTY

  constructor(
    reader: ReadableStreamDefaultReader<Uint8Array>,
    callContext: {
      telefunctionName: string
      telefuncFilePath: string
      abortController: AbortController
    },
  ) {
    super(callContext)
    this.reader = reader

    // When the fetch is aborted, cancel the reader first to prevent the browser
    // from generating a spurious unhandled "BodyStreamBuffer was aborted" rejection.
    callContext.abortController.signal.addEventListener('abort', () => reader.cancel(), { once: true })
  }

  cancel(): void {
    this.cancelled = true
    this.reader.cancel()
  }

  async readExact(n: number): Promise<Uint8Array> {
    while (this.buffer.length < n) {
      let done: boolean
      let value: Uint8Array | undefined
      let readError: unknown
      try {
        ;({ done, value } = await this.reader.read())
      } catch (err) {
        readError = err
        done = true
      }
      if (done) {
        if (this.callContext.abortController.signal.aborted) throwCancelError()
        if (this.cancelled) return EMPTY
        throw readError ?? new Error('Connection lost — the server closed the stream before all data was received.')
      }
      this.buffer = this.buffer.length === 0 ? value! : concat(this.buffer, value!)
    }
    const result = this.buffer.subarray(0, n)
    this.buffer = n < this.buffer.length ? this.buffer.subarray(n) : EMPTY
    return result
  }
}
