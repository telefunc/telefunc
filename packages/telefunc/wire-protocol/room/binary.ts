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
/** Binary relay format: `[16-byte member UUID][1-byte flags][?1-byte track length + track][?2-byte meta length + meta][payload]`. A plain publish costs one flag byte; named tracks (mic/camera/screen
 * on one member lane) and optional per-frame `meta` ride only when set, so media multiplexing needs no hand-rolled envelopes.
 */
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
/** Split a binary relay frame into sender, track, per-frame meta, and payload. The one validating seam for an untrusted frame (a hand-crafted publish need not have gone through `frameWithMemberId`):
 * `null` on truncation, an empty named track (`''` is the default lane), or metadata that isn't valid serialized JSON. A `null` return means "reject this frame" — callers never see a half-parsed one.
 */
function unframeMemberId(data: Uint8Array): {
  from: string
  payload: Uint8Array
  track: string | null
  meta: Record<string, unknown> | null
  retain: boolean
} | null {
  if (data.byteLength < MEMBER_ID_BYTE_LENGTH + 1) return null
  const flags = data[MEMBER_ID_BYTE_LENGTH]!
  if (flags & ~FRAME_FLAGS_KNOWN) return null // an unknown flag bit — malformed or forward-incompatible; reject it
  const retain = (flags & FRAME_FLAG_RETAIN) !== 0
  let track: string | null = null
  let meta: Record<string, unknown> | null = null
  let offset = MEMBER_ID_BYTE_LENGTH + 1
  if (flags & FRAME_FLAG_TRACK) {
    if (data.byteLength < offset + 1) return null
    const trackLength = data[offset]!
    offset += 1
    if (trackLength === 0 || data.byteLength < offset + trackLength) return null
    try {
      track = frameTextDecoder.decode(data.subarray(offset, offset + trackLength))
    } catch {
      return null
    }
    offset += trackLength
  }
  if (flags & FRAME_FLAG_META) {
    if (data.byteLength < offset + 2) return null
    const metaLength = (data[offset]! << 8) | data[offset + 1]!
    offset += 2
    if (data.byteLength < offset + metaLength) return null
    try {
      const parsedMeta: unknown = parse(frameTextDecoder.decode(data.subarray(offset, offset + metaLength)))
      if (!isRecord(parsedMeta)) return null // meta must be a record — scalars/arrays are rejected
      meta = parsedMeta
    } catch {
      return null // malformed meta JSON on a hand-crafted frame — reject the whole frame, don't throw
    }
    offset += metaLength
  }
  return { from: bytesToUuid(data), payload: data.subarray(offset), track, meta, retain }
}
/** The sender UUID carried in a binary frame's fixed prefix, or `null` if the frame is too short to
 *  hold one. Cheap (reads only the member-ID prefix): lets the publish path check membership before
 *  the full validating `unframeMemberId`, so a frame is parsed once, not twice. */
function binaryFrameSender(data: Uint8Array): string | null {
  return data.byteLength >= MEMBER_ID_BYTE_LENGTH ? bytesToUuid(data) : null
}
// Binary wants — per member, per track
/** The default (unnamed) track's slot in want sets and key routing — track names are non-empty by contract (`frameWithMemberId`), so `''` is unambiguous. */
const DEFAULT_TRACK = ''
/** Which of a publisher's tracks a holder wants: every track, or an exact set (`DEFAULT_TRACK` selects the unnamed lane). */
type TrackWants = { all: boolean; tracks: string[] }
/** A holder's complete binary wants. `everyMember` comes from room-level listeners and applies
 *  to all members; `members` adds participant-scoped wants on top. This one shape drives all
 *  three gates: the client's declaration, the server's upstream key set, and the per-stub relay. */
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
/** Does a complete binary want cover one (member, track)? The one predicate behind both the live
 *  relay gate (`RoomStubChannel._wantsBinary`) and retained-frame replay: `everyMember` applies to
 *  all members, `members` adds participant-scoped wants on top. */
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
  if (!isRecord(wants)) return null
  const everyMember = sanitizeTrackWants(wants.everyMember)
  if (!everyMember || !isRecord(wants.members)) return null
  const members: Record<string, TrackWants> = Object.create(null)
  const entries = Object.entries(wants.members)
  for (const [memberId, trackWants] of entries) {
    if (uuidToBytes(memberId) === null) return null
    const sanitized = sanitizeTrackWants(trackWants)
    if (!sanitized) return null
    members[memberId] = sanitized
  }
  return { everyMember, members }
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
