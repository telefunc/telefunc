export { lengthOf, FrameReader }

/** A frame on a stream goes behind its byte length, a u32 big-endian. */
function lengthOf(frame: Uint8Array): Uint8Array<ArrayBuffer> {
  const length = new Uint8Array(4)
  new DataView(length.buffer).setUint32(0, frame.byteLength)
  return length
}

/** The frames of one stream, from its chunks as they arrive. */
class FrameReader {
  private readonly maxFrameBytes: number
  /** What arrived of the frame being read, its length first. */
  private held: Uint8Array[] = []
  private heldBytes = 0

  constructor({ maxFrameBytes }: { maxFrameBytes: number }) {
    this.maxFrameBytes = maxFrameBytes
  }

  /** The frames `chunk` completes, each its own bytes; null once a frame's length passes `maxFrameBytes`. */
  read(chunk: Uint8Array): Uint8Array<ArrayBuffer>[] | null {
    this.held.push(chunk)
    this.heldBytes += chunk.byteLength
    const frames: Uint8Array<ArrayBuffer>[] = []
    while (this.heldBytes >= 4) {
      const length = new DataView(this.peek(4).buffer).getUint32(0)
      if (length > this.maxFrameBytes) return null
      if (this.heldBytes < 4 + length) break
      this.take(4)
      frames.push(this.take(length))
    }
    return frames
  }

  private peek(bytes: number): Uint8Array<ArrayBuffer> {
    const out = new Uint8Array(bytes)
    let at = 0
    for (const part of this.held) {
      const n = Math.min(part.byteLength, bytes - at)
      out.set(part.subarray(0, n), at)
      at += n
      if (at === bytes) break
    }
    return out
  }

  private take(bytes: number): Uint8Array<ArrayBuffer> {
    const out = this.peek(bytes)
    let left = bytes
    while (left > 0) {
      const part = this.held[0]!
      if (part.byteLength > left) {
        this.held[0] = part.subarray(left)
        break
      }
      this.held.shift()
      left -= part.byteLength
    }
    this.heldBytes -= bytes
    return out
  }
}
