export { MESSAGE_BYTES, textMessage, binaryMessage, Tally }
export type { TallyState }

/** 16 KiB, so a few dozen messages are past what a replay buffer once kept of what was on the wire. */
const MESSAGE_BYTES = 16_384

function textMessage(n: number): string {
  return String(n).padEnd(MESSAGE_BYTES, '.')
}

function binaryMessage(n: number): Uint8Array {
  const bytes = new Uint8Array(MESSAGE_BYTES)
  new DataView(bytes.buffer).setUint32(0, n)
  return bytes
}

type TallyState = { text: number; binary: number; inOrder: boolean }

/** Counts the numbered messages that arrive, and whether each is the one after the last. */
class Tally {
  readonly state: TallyState = { text: 0, binary: 0, inOrder: true }

  /** Returns the message's number. */
  text(message: string): number {
    const n = Number.parseInt(message)
    if (n !== this.state.text) this.state.inOrder = false
    this.state.text++
    return n
  }

  binary(message: Uint8Array): number {
    const n = new DataView(message.buffer, message.byteOffset).getUint32(0)
    if (n !== this.state.binary) this.state.inOrder = false
    this.state.binary++
    return n
  }
}
