export { HEADER, SEQ_OFFSET, SEQ_SPAN, writeHeader, writeU16, writeU32, writeU64, readU16, readU32, readU64 }

// ===== Wire protocol =====
//
// Every frame has a uniform 7-byte header:
//   [u8 tag][u16 LE index][u32 LE seq][payload...]
//
// `tag` discriminates the frame variant (data, wire ctrl, per-channel ctrl).
// `index` is the channel ix for per-channel frames; 0 for wire-level frames.
// `seq` is the replay sequence number of a sequenced frame: a data frame, or a CLOSE, CLOSE_ACK, ABORT or ERROR. Other
// ctrl frames carry 0, but a LENGTH and a WebSocket's PING, PONG and ANNOUNCE, which say a count there. Seqs count on
// past 32 bits, and a u32 seq on the wire is their low 32 bits, which `decode` returns as they are: the frame's owner
// reads the whole seq from where it stands on the channel (`seqNear`, `seqThrough`).
//
// Channel indices are client-owned and stable for the channel's lifetime.
// Sequence numbers are sender-assigned for replayable data frames in both directions.
// Each side tracks the highest seq received and replays after reconnect via reconcile.

const HEADER = 7
/** Where a header's u32 starts: a frame's seq, or the count a PING, PONG or ANNOUNCE carries in its place. */
const SEQ_OFFSET = 3
/** How many seqs a u32 tells apart. */
const SEQ_SPAN = 2 ** 32

function writeHeader(frame: Uint8Array, tag: number, index: number, seq: number): void {
  frame[0] = tag
  writeU16(frame, 1, index)
  writeU32(frame, SEQ_OFFSET, seq)
}

function writeU16(frame: Uint8Array, offset: number, n: number): void {
  frame[offset] = n & 0xff
  frame[offset + 1] = (n >> 8) & 0xff
}

function writeU32(frame: Uint8Array, offset: number, n: number): void {
  frame[offset] = n & 0xff
  frame[offset + 1] = (n >> 8) & 0xff
  frame[offset + 2] = (n >> 16) & 0xff
  frame[offset + 3] = (n >> 24) & 0xff
}

function readU16(buf: Uint8Array, offset: number): number {
  return (buf[offset] as number) | ((buf[offset + 1] as number) << 8)
}

/** u32 low, then u32 high: exact to 2^53, past which a JS number skips integers. */
function writeU64(frame: Uint8Array, offset: number, n: number): void {
  writeU32(frame, offset, n)
  writeU32(frame, offset + 4, n / SEQ_SPAN)
}

function readU32(buf: Uint8Array, offset: number): number {
  return (
    ((buf[offset] as number) |
      ((buf[offset + 1] as number) << 8) |
      ((buf[offset + 2] as number) << 16) |
      ((buf[offset + 3] as number) << 24)) >>>
    0
  )
}

function readU64(buf: Uint8Array, offset: number): number {
  return readU32(buf, offset) + readU32(buf, offset + 4) * SEQ_SPAN
}
