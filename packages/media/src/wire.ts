export {
  MEDIA_TRACK,
  UNIT_HEADER_BYTES,
  audioTrackOf,
  videoTrackOf,
  encodeUnit,
  decodeUnit,
  encodeAudioPayload,
  decodeAudioPayload,
  wallClock,
  ticksSince,
  isMediaMessage,
}
export type { UnitHeader, AudioPayload, Catalog, SourceCatalog, VideoSize, KeyFrameRequest, MediaMessage }

/** The text track of a member's catalog, retained so a page that starts watching reads it first, and of the key
 *  frames its page asks other members for. */
const MEDIA_TRACK = 'media'

/** Each frame's Room payload begins with this: whether a decoder can start at it, where it stands in its group, and when
 *  it was captured. Room's `meta` would cost a JSON object per frame. */
const UNIT_HEADER_BYTES = 12

const FLAG_KEY = 0b0000_0001

/** What a unit's header says. `group` and `index` number a video track's key-frame groups and the frames in each;
 *  on an audio track, `group` numbers its frames and `level` is their loudness in −dBov (RFC 6464). */
type UnitHeader = {
  key: boolean
  level: number
  group: number
  index: number
  /** In 0.1 ms ticks of the publisher's wall clock, mod 2³²: see `wallClock`. */
  captured: number
}

/** A video track carries one size of a source; its name says which. */
function videoTrackOf(source: string, height: number): string {
  return `${source}@${height}`
}

function audioTrackOf(source: string): string {
  return `${source}@audio`
}

function encodeUnit(header: UnitHeader, payload: Uint8Array): Uint8Array {
  const unit = new Uint8Array(UNIT_HEADER_BYTES + payload.byteLength)
  const view = new DataView(unit.buffer)
  view.setUint8(0, header.key ? FLAG_KEY : 0)
  view.setUint8(1, header.level)
  view.setUint32(2, header.group)
  view.setUint16(6, header.index)
  view.setUint32(8, header.captured)
  unit.set(payload, UNIT_HEADER_BYTES)
  return unit
}

/** Null for bytes no publisher of this package framed. */
function decodeUnit(unit: Uint8Array): { header: UnitHeader; payload: Uint8Array } | null {
  if (unit.byteLength < UNIT_HEADER_BYTES) return null
  const view = new DataView(unit.buffer, unit.byteOffset, unit.byteLength)
  const header: UnitHeader = {
    key: (view.getUint8(0) & FLAG_KEY) !== 0,
    level: view.getUint8(1),
    group: view.getUint32(2),
    index: view.getUint16(6),
    captured: view.getUint32(8),
  }
  return { header, payload: unit.subarray(UNIT_HEADER_BYTES) }
}

/** An audio frame, and the one before it, so a viewer that lost one packet plays it from the next (RFC 2198's
 *  redundancy, one frame deep, as Chrome's WebRTC sends it). */
type AudioPayload = {
  frame: Uint8Array
  /** Empty for a track's first frame. */
  previous: Uint8Array
  /** In 0.1 ms ticks, how long before `frame` the previous frame was captured. */
  previousAgo: number
}

function encodeAudioPayload({ frame, previous, previousAgo }: AudioPayload): Uint8Array {
  const payload = new Uint8Array(4 + frame.byteLength + previous.byteLength)
  const view = new DataView(payload.buffer)
  view.setUint16(0, frame.byteLength)
  view.setUint16(2, Math.min(previousAgo, 0xffff))
  payload.set(frame, 4)
  payload.set(previous, 4 + frame.byteLength)
  return payload
}

function decodeAudioPayload(payload: Uint8Array): AudioPayload | null {
  if (payload.byteLength < 4) return null
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength)
  const length = view.getUint16(0)
  if (payload.byteLength < 4 + length) return null
  return {
    frame: payload.subarray(4, 4 + length),
    previous: payload.subarray(4 + length),
    previousAgo: view.getUint16(2),
  }
}

/** Now on the wall clock, in 0.1 ms ticks mod 2³²: pages on one machine share it, and it wraps after five days. */
function wallClock(): number {
  return Math.round((performance.timeOrigin + performance.now()) * 10) >>> 0
}

/** In ms, how long after tick `earlier` tick `later` came, across the wrap. */
function ticksSince(later: number, earlier: number): number {
  return ((later - earlier) | 0) / 10
}

type VideoSize = { track: string; width: number; height: number }

type SourceCatalog = {
  video?: { codec: string; sizes: VideoSize[] }
  audio?: { track: string; sampleRate: number; numberOfChannels: number }
}

/** A member's sources by name, as its page publishes them. */
type Catalog = { __media: 'catalog'; sources: Record<string, SourceCatalog> }

/** A page asks member `to` for a key frame on `track`: it starts watching it, or lost a frame of it. */
type KeyFrameRequest = { __media: 'key'; to: string; track: string }

type MediaMessage = Catalog | KeyFrameRequest

function isMediaMessage(message: unknown): message is MediaMessage {
  const kind = (message as { __media?: unknown } | null)?.__media
  return kind === 'catalog' || kind === 'key'
}
