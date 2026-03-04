export { SSEStreamReader }

import { BaseStreamReader } from './BaseStreamReader.js'
import { concat } from '../../frame.js'
import { base64urlToUint8Array } from '../../base64url.js'
import { throwCancelError } from '../../../client/remoteTelefunctionCall/errors.js'

const EMPTY = new Uint8Array(0)

/** SSE transport reader — decodes `data: <base64url>\n\n` events back to
 *  binary frames.
 *
 *  `readExact` is the only method implemented here. SSE line parser state
 *  (`lineBuf`, `pendingData`, `TextDecoder`) lives as instance fields,
 *  persisted across calls. `reader.read()` is called inline — only when more
 *  bytes are needed — preserving the full pull chain through
 *  `FrameDemuxer.ensureReading` to the server generator. No separate pump. */
class SSEStreamReader extends BaseStreamReader {
  private reader: ReadableStreamDefaultReader<Uint8Array>
  private binary: Uint8Array = EMPTY
  private decoder = new TextDecoder()
  private lineBuf = ''
  private pendingData = ''

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
    callContext.abortController.signal.addEventListener('abort', () => reader.cancel(), { once: true })
  }

  cancel(): void {
    this.cancelled = true
    this.reader.cancel()
  }

  async readExact(n: number): Promise<Uint8Array> {
    while (this.binary.length < n) {
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
        throw readError ?? new Error('Connection lost — server closed the SSE stream before all data was received.')
      }
      this.lineBuf += this.decoder.decode(value!, { stream: true })
      const lines = this.lineBuf.split('\n')
      this.lineBuf = lines.pop()!
      for (const line of lines) {
        if (line.startsWith('data: ')) {
          this.pendingData = line.slice(6)
        } else if (line === '' && this.pendingData !== '') {
          this.binary = concat(this.binary, base64urlToUint8Array(this.pendingData))
          this.pendingData = ''
        }
      }
    }
    const result = this.binary.subarray(0, n)
    this.binary = n < this.binary.length ? this.binary.subarray(n) : EMPTY
    return result
  }
}
