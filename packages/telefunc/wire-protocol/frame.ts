export { encodeU32, decodeU32, concat, encodeIndexedFrame, decodeIndexedFrame, textEncoder, textDecoder }

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

const U32_SIZE = 4 // byte size of a uint32

function encodeU32(n: number): Uint8Array<ArrayBuffer> {
  const buf = new Uint8Array(U32_SIZE)
  new DataView(buf.buffer).setUint32(0, n, false)
  return buf as Uint8Array<ArrayBuffer>
}

function decodeU32(buf: Uint8Array, offset = 0): number {
  return new DataView(buf.buffer, buf.byteOffset + offset, U32_SIZE).getUint32(0, false)
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  const result = new Uint8Array(a.length + b.length)
  result.set(a, 0)
  result.set(b, a.length)
  return result
}

// ===== Indexed frame (streaming multiplexing) =====
// Wire format: [u32 body_len][u8 index][payload]
// body_len covers the index byte + payload (excludes the u32 length prefix itself)

const INDEX_BYTES = 1 // u8 stream index

function encodeIndexedFrame(index: number, payload: Uint8Array): Uint8Array {
  const bodyLen = INDEX_BYTES + payload.byteLength
  const frame = new Uint8Array(U32_SIZE + bodyLen)
  new DataView(frame.buffer).setUint32(0, bodyLen, false)
  frame[U32_SIZE] = index
  frame.set(payload, U32_SIZE + INDEX_BYTES)
  return frame
}

/** Decode the body of an indexed frame (after the 4-byte length prefix has been read). */
function decodeIndexedFrame(frameBody: Uint8Array): { index: number; payload: Uint8Array } {
  return { index: frameBody[0]!, payload: frameBody.subarray(INDEX_BYTES) }
}
