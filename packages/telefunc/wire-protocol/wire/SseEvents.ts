export { SseEvents }

import { base64urlToUint8Array } from './base64url.js'

/** Splits an SSE body into its events' base64url data, searching each read once: a line that spans reads is kept in
 *  parts and joined once. */
class SseEvents {
  private decoder = new TextDecoder()
  private partial: string[] = []
  private data = ''

  /** Calls `onEvent` with each event `bytes` completes. Returns whether `bytes` carried any of an event: a read of
   *  comments alone says nothing of the server (Telefunc's sends one as the stream opens; something in front of it may
   *  send more, to keep the connection open). */
  push(bytes: Uint8Array, onEvent: (event: Uint8Array<ArrayBuffer>) => void): boolean {
    const text = this.decoder.decode(bytes, { stream: true })
    let carried = false
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
        carried = true
      } else if (line === '' && this.data !== '') {
        onEvent(base64urlToUint8Array(this.data))
        this.data = ''
        carried = true
      }
    }
    if (start < text.length) this.partial.push(text.slice(start))
    return carried || this.partway
  }

  /** An event is partly read: the last read ended inside it. */
  get partway(): boolean {
    return this.data !== '' || this.partial[0]?.startsWith(':') === false
  }
}
