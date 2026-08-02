export {
  uuidToBytes,
  frameWithMemberId,
  unframeMemberId,
  binaryFrameSender,
  DEFAULT_TRACK,
  isRoomTrack,
  emptyTrackWants,
  mergeTrackWants,
  wantsAnyBinary,
  binaryWantsCovers,
  sanitizeBinaryWants,
}
export type { TrackWants, BinaryWants }

import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { assert, assertUsage } from '../../utils/assert.js'
import { isRecord } from './model.js'
import type { BinaryPublishOptions } from './types.js'

// Member IDs — UUIDs, framed as a fixed 16-byte prefix on binary messages
const MEMBER_ID_BYTE_LENGTH = 16
const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
const BYTE_TO_HEX: string[] = []
for (let byte = 0; byte < 256; byte++) BYTE_TO_HEX.push(byte.toString(16).padStart(2, '0'))
/** Canonical UUID string → 16 bytes. Returns `null` for anything else. */
function uuidToBytes(uuid: string): Uint8Array | null {
  if (!UUID_REGEX.test(uuid)) return null
  const hex = uuid.split('-').join('')
  const bytes = new Uint8Array(MEMBER_ID_BYTE_LENGTH)
  for (let i = 0; i < MEMBER_ID_BYTE_LENGTH; i++) bytes[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16)
  return bytes
}
/** First 16 bytes → canonical lowercase UUID string. */
function bytesToUuid(bytes: Uint8Array): string {
  let uuid = ''
  for (let i = 0; i < MEMBER_ID_BYTE_LENGTH; i++) {
    if (i === 4 || i === 6 || i === 8 || i === 10) uuid += '-'
    uuid += BYTE_TO_HEX[bytes[i]!]!
  }
  return uuid
}
/** Binary frame flags (one byte after the member ID). */
const FRAME_FLAG_META = 0b0000_0001
const FRAME_FLAG_TRACK = 0b0000_0010
const FRAME_FLAG_RETAIN = 0b0000_0100
/** The only bits the framer defines. Any other bit set is a malformed or forward-incompatible frame. */
const FRAME_FLAGS_KNOWN = FRAME_FLAG_META | FRAME_FLAG_TRACK | FRAME_FLAG_RETAIN
/** Structural maxima of the one-byte track-length and two-byte metadata-length fields. */
const TRACK_LENGTH_FIELD_MAX = 0xff
const META_LENGTH_FIELD_MAX = 0xffff
const frameTextEncoder = /* @__PURE__ */ new TextEncoder()
const frameTextDecoder = /* @__PURE__ */ new TextDecoder('utf-8', { fatal: true, ignoreBOM: true })
function frameWithMemberId(memberId: string, payload: Uint8Array, opts?: BinaryPublishOptions): Uint8Array {
  const idBytes = uuidToBytes(memberId)
  assert(idBytes, 'room member IDs are UUIDs')
  let flags = opts?.retain === true ? FRAME_FLAG_RETAIN : 0
  let trackBytes: Uint8Array | null = null
  if (opts?.track !== undefined) {
    assertUsage(
      isRoomTrack(opts.track) && opts.track.length > 0,
      `track should be a non-empty well-formed string of at most ${TRACK_LENGTH_FIELD_MAX} bytes`,
    )
    trackBytes = frameTextEncoder.encode(opts.track)
    flags |= FRAME_FLAG_TRACK
  }
  let metaBytes: Uint8Array | null = null
  if (opts?.meta !== undefined) {
    assertUsage(isRecord(opts.meta), 'publishBinary() meta should be an object')
    metaBytes = frameTextEncoder.encode(stringify(opts.meta))
    assertUsage(
      metaBytes.byteLength <= META_LENGTH_FIELD_MAX,
      `publishBinary() meta should be at most ${META_LENGTH_FIELD_MAX} bytes once serialized`,
    )
    flags |= FRAME_FLAG_META
  }
  const headerLength =
    MEMBER_ID_BYTE_LENGTH +
    1 +
    (trackBytes ? 1 + trackBytes.byteLength : 0) +
    (metaBytes ? 2 + metaBytes.byteLength : 0)
  const framed = new Uint8Array(headerLength + payload.byteLength)
  framed.set(idBytes, 0)
  framed[MEMBER_ID_BYTE_LENGTH] = flags
  let offset = MEMBER_ID_BYTE_LENGTH + 1
  if (trackBytes) {
    framed[offset] = trackBytes.byteLength
    framed.set(trackBytes, offset + 1)
    offset += 1 + trackBytes.byteLength
  }
  if (metaBytes) {
    framed[offset] = (metaBytes.byteLength >> 8) & 0xff
    framed[offset + 1] = metaBytes.byteLength & 0xff
    framed.set(metaBytes, offset + 2)
    offset += 2 + metaBytes.byteLength
  }
  framed.set(payload, headerLength)
  return framed
}
type FrameCursor = { offset: number }
function decodeFrameText(bytes: Uint8Array): string | undefined {
  try {
    return frameTextDecoder.decode(bytes)
  } catch {
    return undefined
  }
}
function readTrackSection(data: Uint8Array, cursor: FrameCursor): string | undefined {
  const length = data[cursor.offset++]
  if (length === undefined || length === 0 || data.byteLength < cursor.offset + length) return undefined
  const track = decodeFrameText(data.subarray(cursor.offset, cursor.offset + length))
  if (track === undefined) return undefined
  cursor.offset += length
  return track
}
function readMetaSection(data: Uint8Array, cursor: FrameCursor): Record<string, unknown> | undefined {
  const length = data.byteLength < cursor.offset + 2 ? -1 : (data[cursor.offset]! << 8) | data[cursor.offset + 1]!
  cursor.offset += 2
  if (length < 0 || data.byteLength < cursor.offset + length) return undefined
  const serialized = decodeFrameText(data.subarray(cursor.offset, cursor.offset + length))
  if (serialized === undefined) return undefined
  try {
    const meta: unknown = parse(serialized)
    if (!isRecord(meta)) return undefined
    cursor.offset += length
    return meta
  } catch {
    return undefined
  }
}
function unframeMemberId(data: Uint8Array): {
  from: string
  payload: Uint8Array
  track: string | null
  meta: Record<string, unknown> | null
  retain: boolean
} | null {
  if (data.byteLength < MEMBER_ID_BYTE_LENGTH + 1) return null
  const flags = data[MEMBER_ID_BYTE_LENGTH]!
  if (flags & ~FRAME_FLAGS_KNOWN) return null
  const cursor = { offset: MEMBER_ID_BYTE_LENGTH + 1 }
  const track = flags & FRAME_FLAG_TRACK ? readTrackSection(data, cursor) : null
  if (track === undefined) return null
  const meta = flags & FRAME_FLAG_META ? readMetaSection(data, cursor) : null
  if (meta === undefined) return null
  return { from: bytesToUuid(data), payload: data.subarray(cursor.offset), track, meta, retain: !!(flags & FRAME_FLAG_RETAIN) }
}
function binaryFrameSender(data: Uint8Array): string | null {
  return data.byteLength >= MEMBER_ID_BYTE_LENGTH ? bytesToUuid(data) : null
}
// Binary wants — per member, per track
/** The default (unnamed) track's slot in want sets and key routing — track names are non-empty by contract (`frameWithMemberId`), so `''` is unambiguous. */
const DEFAULT_TRACK = ''
/** Which of a publisher's tracks a holder wants: every track, or an exact set (`DEFAULT_TRACK` selects the unnamed lane). */
type TrackWants = { all: boolean; tracks: string[] }
type BinaryWants = { everyMember: TrackWants; members: Record<string, TrackWants> }
function emptyTrackWants(): TrackWants {
  return { all: false, tracks: [] }
}
function mergeTrackWants(a: TrackWants, b: TrackWants): TrackWants {
  if (a.all || b.all) return { all: true, tracks: [] }
  return { all: false, tracks: [...new Set([...a.tracks, ...b.tracks])] }
}
function wantsTrack(wants: TrackWants, track: string): boolean {
  return wants.all || wants.tracks.includes(track)
}
function binaryWantsCovers(wants: BinaryWants, memberId: string, track: string): boolean {
  if (wantsTrack(wants.everyMember, track)) return true
  const memberWants = wants.members[memberId]
  return memberWants !== undefined && wantsTrack(memberWants, track)
}
function wantsAnyBinary(wants: BinaryWants): boolean {
  return wants.everyMember.all || wants.everyMember.tracks.length > 0 || Object.keys(wants.members).length > 0
}
/** Validate a client-declared `sub-binary` want (untrusted input), or return `null`. */
function sanitizeBinaryWants(wants: unknown): BinaryWants | null {
  try {
    if (!isRecord(wants)) return null
    const everyMember = sanitizeTrackWants(wants.everyMember)
    if (!everyMember || !isRecord(wants.members)) return null
    const members: Record<string, TrackWants> = Object.create(null)
    for (const [memberId, trackWants] of Object.entries(wants.members)) {
      if (uuidToBytes(memberId) === null) return null
      const sanitized = sanitizeTrackWants(trackWants)
      if (!sanitized) return null
      members[memberId] = sanitized
    }
    return { everyMember, members }
  } catch {
    return null
  }
}
function sanitizeTrackWants(wants: unknown): TrackWants | null {
  if (!isRecord(wants) || typeof wants.all !== 'boolean' || !Array.isArray(wants.tracks)) return null
  // Bound by UTF-8 bytes, the same unit the frame path uses (`frameWithMemberId`) — a `.length` char count could admit a want that doesn't fit the frame's one-byte track-length field.
  if (!wants.tracks.every(isRoomTrack)) return null
  return { all: wants.all, tracks: wants.tracks as string[] }
}
function isRoomTrack(track: unknown): track is string {
  return (
    typeof track === 'string' &&
    track.isWellFormed() &&
    frameTextEncoder.encode(track).byteLength <= TRACK_LENGTH_FIELD_MAX
  )
}
