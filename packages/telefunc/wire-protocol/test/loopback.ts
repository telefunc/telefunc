export { Pipe, lengthPrefixed, postBody }

import { decodeU32 } from '../frame.js'

/** One direction of a link: what is pushed leaves once what was pushed before it has, at `bytesPerMs`, and arrives
 *  `latencyMs` after it left. One delivery hands over `readBytes` at most, as a socket read does: the receiver's
 *  microtasks run between two. */
class Pipe {
  private queue: { bytes: number; at: number; deliver: () => void }[] = []
  private timer: ReturnType<typeof setTimeout> | null = null
  private held = false
  /** What the pipe carries, in bytes: its sender's `bufferedAmount`. */
  bytes = 0
  /** A slow link carries what is pushed only once what was pushed before it has gone through. */
  bytesPerMs = Infinity
  latencyMs: number
  private lastAt = 0

  constructor(
    latencyMs: number,
    private readonly readBytes = Number.POSITIVE_INFINITY,
  ) {
    this.latencyMs = latencyMs
  }

  /** Carries `bytes`, and calls `deliver` once they arrived. */
  push(bytes: number, deliver: () => void): void {
    this.lastAt = Math.max(Date.now(), this.lastAt) + bytes / this.bytesPerMs
    this.queue.push({ bytes, at: this.lastAt + this.latencyMs, deliver })
    this.bytes += bytes
    this.schedule()
  }

  /** What the network still carries stops arriving, until `release`. */
  hold(): void {
    this.held = true
  }

  release(): void {
    this.held = false
    this.schedule()
  }

  /** The link is cut: what it still carries is lost. */
  clear(): void {
    this.queue.length = 0
    this.bytes = 0
  }

  get length(): number {
    return this.queue.length
  }

  private schedule(): void {
    if (this.timer || this.held || this.queue.length === 0) return
    this.timer = setTimeout(
      () => {
        this.timer = null
        let read = 0
        while (!this.held && read < this.readBytes && this.queue.length > 0 && this.queue[0]!.at <= Date.now()) {
          const { bytes, deliver } = this.queue.shift()!
          read += bytes
          this.bytes -= bytes
          deliver()
        }
        this.schedule()
      },
      Math.max(0, this.queue[0]!.at - Date.now()),
    )
  }
}

/** `[u32 length][bytes]` chunks, each `prefixed` and its `frame`: an SSE POST's metadata header, then its frames. */
function lengthPrefixed(bytes: Uint8Array): { prefixed: Uint8Array; frame: Uint8Array }[] {
  const chunks: { prefixed: Uint8Array; frame: Uint8Array }[] = []
  let offset = 0
  while (offset + 4 <= bytes.length) {
    const length = decodeU32(bytes.subarray(offset, offset + 4) as Uint8Array<ArrayBuffer>)
    chunks.push({
      prefixed: bytes.subarray(offset, offset + 4 + length),
      frame: bytes.subarray(offset + 4, offset + 4 + length),
    })
    offset += 4 + length
  }
  return chunks
}

/** An SSE POST's Blob body: its metadata header, then its frames. */
async function postBody(
  blob: Blob,
): Promise<{ metadata: { connId: string; streamResponse?: boolean }; frames: Uint8Array[] }> {
  const [metadata, ...frames] = lengthPrefixed(new Uint8Array(await blob.arrayBuffer())).map(({ frame }) => frame)
  return { metadata: JSON.parse(new TextDecoder().decode(metadata)), frames }
}
