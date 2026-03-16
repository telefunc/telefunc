export {
  encodeU32,
  decodeU32,
  concat,
  encodeLengthPrefixedFrames,
  encodeIndexedFrame,
  decodeIndexedFrame,
  encodeRequestEnvelope,
  textEncoder,
  textDecoder,
}

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

const U32_SIZE = 4 // byte size of a uint32

type RequestEnvelopePart = Blob | Uint8Array<ArrayBuffer>

function encodeU32(n: number): Uint8Array<ArrayBuffer> {
  const buf = new Uint8Array(U32_SIZE)
  new DataView(buf.buffer).setUint32(0, n, false)
  return buf
}

function decodeU32(buf: Uint8Array<ArrayBuffer>, offset = 0): number {
  return new DataView(buf.buffer, buf.byteOffset + offset, U32_SIZE).getUint32(0, false)
}

function concat(a: Uint8Array<ArrayBuffer>, b: Uint8Array<ArrayBuffer>): Uint8Array<ArrayBuffer> {
  const result = new Uint8Array(a.length + b.length)
  result.set(a, 0)
  result.set(b, a.length)
  return result
}

/** Encode a batch of frames as [u32 len][frame][u32 len][frame]... using the shared u32 codec. */
function encodeLengthPrefixedFrames(frames: Uint8Array<ArrayBuffer>[]): Uint8Array<ArrayBuffer>
function encodeLengthPrefixedFrames<T>(
  frames: T[],
  getFrame: (entry: T) => Uint8Array<ArrayBuffer>,
): Uint8Array<ArrayBuffer>
function encodeLengthPrefixedFrames<T>(
  frames: (Uint8Array<ArrayBuffer> | T)[],
  getFrame?: (entry: T) => Uint8Array<ArrayBuffer>,
): Uint8Array<ArrayBuffer> {
  const resolveFrame = (entry: Uint8Array<ArrayBuffer> | T): Uint8Array<ArrayBuffer> => {
    return getFrame ? getFrame(entry as T) : (entry as Uint8Array<ArrayBuffer>)
  }
  let total = 0
  for (const entry of frames) {
    const frame = resolveFrame(entry)
    total += U32_SIZE + frame.byteLength
  }
  const out = new Uint8Array(total)
  let offset = 0
  for (const entry of frames) {
    const frame = resolveFrame(entry)
    out.set(encodeU32(frame.byteLength), offset)
    offset += U32_SIZE
    out.set(frame, offset)
    offset += frame.byteLength
  }
  return out
}

/** Shared by Telefunc binary uploads and SSE requests: [u32 metadata length][metadata UTF-8][parts...] */
function encodeRequestEnvelope(metadataSerialized: string, parts: RequestEnvelopePart[]): Blob {
  const metadataBytes = textEncoder.encode(metadataSerialized)
  return new Blob([encodeU32(metadataBytes.length), metadataBytes, ...parts])
}

// ===== Indexed frame (streaming multiplexing) =====
// Wire format: [u32 body_len][u8 index][payload]
// body_len covers the index byte + payload (excludes the u32 length prefix itself)

const INDEX_BYTES = 1 // u8 stream index

function encodeIndexedFrame(index: number, payload: Uint8Array<ArrayBuffer>): Uint8Array<ArrayBuffer> {
  const bodyLen = INDEX_BYTES + payload.byteLength
  const frame = new Uint8Array(U32_SIZE + bodyLen)
  new DataView(frame.buffer).setUint32(0, bodyLen, false)
  frame[U32_SIZE] = index
  frame.set(payload, U32_SIZE + INDEX_BYTES)
  return frame
}

/** Decode the body of an indexed frame (after the 4-byte length prefix has been read). */
function decodeIndexedFrame(frameBody: Uint8Array<ArrayBuffer>): {
  index: number
  payload: Uint8Array<ArrayBuffer>
} {
  return { index: frameBody[0]!, payload: frameBody.subarray(INDEX_BYTES) }
}
