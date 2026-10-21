export { SSEEvents }

import { base64urlToUint8Array } from '../wire/base64url.js'

/** Splits an SSE body into its events' base64url data, searching each read once: a line that spans reads is kept in
 *  parts and joined once. */
class SSEEvents {
  private decoder = new TextDecoder()
  private partial: string[] = []
  private data = ''

  /** Calls `onEvent` with each event `bytes` completes. */
  push(bytes: Uint8Array, onEvent: (event: Uint8Array<ArrayBuffer>) => void): void {
    const text = this.decoder.decode(bytes, { stream: true })
    let start = 0
    for (let nl = text.indexOf('\n'); nl !== -1; nl = text.indexOf('\n', start)) {
      let line = text.slice(start, nl)
      if (this.partial.length > 0) {
        this.partial.push(line)
        line = this.partial.join('')
        this.partial = []
      }
      start = nl + 1
      if (line.startsWith('data: ')) {
        this.data = line.slice(6)
      } else if (line === '' && this.data !== '') {
        onEvent(base64urlToUint8Array(this.data))
        this.data = ''
      }
    }
    if (start < text.length) this.partial.push(text.slice(start))
  }
}
