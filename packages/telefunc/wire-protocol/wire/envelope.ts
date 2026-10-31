export {
  encodeU32,
  decodeU32,
  concat,
  concatAll,
  splitOwned,
  encodeLengthPrefixedString,
  encodeIndexedFrame,
  decodeIndexedFrame,
  ownIndexedPayload,
  encodeRequestEnvelope,
  encodeSseRequest,
  encodeSseRequestMetadata,
  encodeLengthPrefixedBlob,
  parseSseRequestMetadata,
  textEncoder,
  textDecoder,
}
export type { SseRequestMetadata, WireSseRequestMetadata }

import { assertProtocol } from './protocolViolation.js'

const textEncoderInstance = new TextEncoder()
const textEncoder = {
  encode: (input?: string): Uint8Array<ArrayBuffer> => textEncoderInstance.encode(input) as Uint8Array<ArrayBuffer>,
}
const textDecoder = new TextDecoder()

const U32_SIZE = 4 // byte size of a uint32
const EMPTY = new Uint8Array(0)

type RequestEnvelopePart = Blob | Uint8Array<ArrayBuffer>

function encodeU32(n: number): Uint8Array<ArrayBuffer> {
  const buf = new Uint8Array(U32_SIZE)
  writeU32(buf, 0, n)
  return buf
}

/** Writes `n` as a big-endian u32 at `at`, as `DataView.setUint32` does, without a DataView per call. */
function writeU32(bytes: Uint8Array, at: number, n: number): void {
  bytes[at] = n >>> 24
  bytes[at + 1] = n >>> 16
  bytes[at + 2] = n >>> 8
  bytes[at + 3] = n
}

function decodeU32(buf: Uint8Array<ArrayBuffer>, offset = 0): number {
  return new DataView(buf.buffer, buf.byteOffset + offset, U32_SIZE).getUint32(0, false)
}

function concat(...parts: Uint8Array<ArrayBuffer>[]): Uint8Array<ArrayBuffer> {
  if (parts.length === 2) {
    const [a, b] = parts as [Uint8Array<ArrayBuffer>, Uint8Array<ArrayBuffer>]
    const result = new Uint8Array(a.length + b.length)
    result.set(a, 0)
    result.set(b, a.length)
    return result
  }
  return concatAll(parts)
}

/** `concat` for a list a spread call can't pass: past about 100k parts it overflows the stack. */
function concatAll(parts: readonly Uint8Array<ArrayBuffer>[]): Uint8Array<ArrayBuffer> {
  let total = 0
  for (const p of parts) total += p.length
  const result = new Uint8Array(total)
  let offset = 0
  for (const p of parts) {
    result.set(p, offset)
    offset += p.length
  }
  return result
}

/** The first `n` bytes of `parts` in a buffer of their own, and the bytes after them. */
function splitOwned(
  parts: readonly Uint8Array<ArrayBuffer>[],
  n: number,
): [bytes: Uint8Array<ArrayBuffer>, rest: Uint8Array<ArrayBuffer>] {
  const bytes = new Uint8Array(n)
  let offset = 0
  for (let i = 0; i < parts.length; i++) {
    const p = parts[i]!
    const take = Math.min(p.length, n - offset)
    bytes.set(p.subarray(0, take), offset)
    offset += take
    if (take < p.length) {
      const rest = parts.slice(i)
      rest[0] = p.subarray(take)
      return [bytes, rest.length === 1 ? rest[0] : concatAll(rest)]
    }
  }
  return [bytes, EMPTY]
}

/** Shared by Telefunc binary uploads and SSE requests: [u32 metadata length][metadata UTF-8][parts...] */
function encodeRequestEnvelope(metadataSerialized: string, parts: RequestEnvelopePart[]): Blob {
  const metadataBytes = textEncoder.encode(metadataSerialized)
  return new Blob([encodeU32(metadataBytes.length), metadataBytes, ...parts])
}

/** Encode a string as `[u32 BE byte length][UTF-8 bytes]`. */
function encodeLengthPrefixedString(s: string): Uint8Array<ArrayBuffer> {
  const bytes = textEncoder.encode(s)
  const out = new Uint8Array(U32_SIZE + bytes.byteLength)
  writeU32(out, 0, bytes.byteLength)
  out.set(bytes, U32_SIZE)
  return out
}

// ===== SSE requests =====

/** The metadata header that opens every SSE POST. Its `kind`:
 *  - `batch`: short outbox batch POST. Body ends quickly, so the server defers `reconciled`
 *    to body-end — dispatched frames lift each channel's `_lastClientSeq` first, giving
 *    accurate `lastSeq` numbers.
 *  - `streamResponse`: opens the server→client `text/event-stream` downstream wire.
 *  - `streamRequest`: the long-lived client→server upload POST. Its body never ends, so the
 *    server emits `reconciled` inline instead of deferring.
 *  - `close`: the page leaves for good, so the server closes the wire as a WebSocket close would. */
type SseRequestMetadata = {
  connId: string
  kind: 'batch' | 'streamResponse' | 'streamRequest' | 'close'
}

/** On the wire, a kind other than `batch` is its name set to `true`. */
const SSE_REQUEST_FLAGS = ['streamResponse', 'streamRequest', 'close'] as const satisfies SseRequestMetadata['kind'][]

/** The metadata as the page builds it, in its wire shape: `{ connId }` for a `batch`, else its kind set to `true`. */
type WireSseRequestMetadata =
  | { connId: string }
  | {
      [Kind in (typeof SSE_REQUEST_FLAGS)[number]]: { connId: string } & Record<Kind, true>
    }[(typeof SSE_REQUEST_FLAGS)[number]]

/** `[u32 length][metadata UTF-8]` — the wire header, shared by every POST kind. The
 *  streaming stream-request POST pushes this onto its body directly (its body can't be a
 *  one-shot Blob); the others hand it to `encodeSseRequest` with their frames appended. */
function encodeSseRequestMetadata(metadata: WireSseRequestMetadata): Uint8Array<ArrayBuffer> {
  return encodeLengthPrefixedString(JSON.stringify(metadata))
}

/** A POST's body: its metadata, then `frames` length-prefixed after `head`, in a Blob that takes the frames as they
 *  are: a batch POST of a window's worth isn't copied into one buffer first. `head`: the first frames,
 *  length-prefixed already. */
function encodeSseRequest(metadata: WireSseRequestMetadata, frames?: Uint8Array<ArrayBuffer>[], head?: Blob): Blob {
  const header = encodeSseRequestMetadata(metadata)
  return new Blob(frames ? [header, encodeLengthPrefixedBlob(frames, head)] : [header])
}

/** `frames` length-prefixed after `head`, in a Blob that takes them as they are. */
function encodeLengthPrefixedBlob(frames: Uint8Array<ArrayBuffer>[], head?: Blob): Blob {
  const parts: (Blob | Uint8Array<ArrayBuffer>)[] = head ? [head] : []
  const lengths = new Uint8Array(U32_SIZE * frames.length)
  for (let i = 0; i < frames.length; i++) {
    const frame = frames[i]!
    const at = U32_SIZE * i
    writeU32(lengths, at, frame.byteLength)
    parts.push(lengths.subarray(at, at + U32_SIZE), frame)
  }
  return new Blob(parts)
}

function parseSseRequestMetadata(value: unknown): SseRequestMetadata {
  assertProtocol(value !== null && typeof value === 'object', 'SSE request metadata is not an object')
  const metadata = value as Record<string, unknown>
  const { connId } = metadata
  assertProtocol(typeof connId === 'string' && connId.length > 0, 'SSE request connId')
  let kind: SseRequestMetadata['kind'] = 'batch'
  for (const flag of SSE_REQUEST_FLAGS) {
    if (metadata[flag] !== true) continue
    assertProtocol(kind === 'batch', 'SSE request of two kinds')
    kind = flag
  }
  return { connId, kind }
}

// ===== Indexed frame (streaming multiplexing) =====
// Wire format: [u32 body_len][u8 index][payload]
// body_len covers the index byte + payload (excludes the u32 length prefix itself)

const INDEX_BYTES = 1 // u8 stream index

function encodeIndexedFrame(index: number, payload: Uint8Array<ArrayBuffer>): Uint8Array<ArrayBuffer> {
  const bodyLen = INDEX_BYTES + payload.byteLength
  const frame = new Uint8Array(U32_SIZE + bodyLen)
  writeU32(frame, 0, bodyLen)
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

/** A decoded indexed frame's `payload` in a buffer the app may transfer: copied unless its buffer holds nothing but
 *  the payload and its own frame's header. */
function ownIndexedPayload(payload: Uint8Array<ArrayBuffer>): Uint8Array<ArrayBuffer> {
  const alone =
    payload.byteOffset <= U32_SIZE + INDEX_BYTES &&
    payload.byteOffset + payload.byteLength === payload.buffer.byteLength
  return alone ? payload : payload.slice()
}
