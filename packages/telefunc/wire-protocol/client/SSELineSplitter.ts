export { SSELineSplitter }

/** Splits an SSE body into lines, searching each read once: a line that spans reads is kept in parts and joined once. */
class SSELineSplitter {
  private decoder = new TextDecoder()
  private partial: string[] = []

  /** Calls `onLine` with each line `bytes` completes. */
  push(bytes: Uint8Array, onLine: (line: string) => void): void {
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
      onLine(line)
    }
    if (start < text.length) this.partial.push(text.slice(start))
  }
}
