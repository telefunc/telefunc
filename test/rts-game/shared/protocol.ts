// The binary wire format for the match's world-state broadcast.
//
// A production RTS moves its state over the binary lane, not JSON: at 10 Hz with hundreds of
// entities, a per-tick JSON snapshot would be an order of magnitude larger and cost a
// parse-the-world on every frame. So the authority packs each per-team, fog-filtered snapshot
// into this compact layout and ships it with `participant.publishBinary(bytes, { track, keyFrame })`.
//
// ── finding #4 (binary framing) — DOCS (I overstated it) ──────────────────────────────────────
// Per-frame the lane gives `{ track, keyFrame }` (`BinaryFrameInfo`) PLUS the subscriber's
// `ChannelPublishInfo` — which already carries `seq` (strict per-key counter, for gap detection)
// and `timestamp`. So delta-baseline reconciliation has what it needs at the receiver; the bytes
// below are genuine game state, and the in-band `tick` is the game's logical clock (for
// interpolation), not a framing workaround. There's no real gap. See README finding #4.
// ─────────────────────────────────────────────────────────────────────────────────────────────

export type { SnapEntity, SnapEvent, Frame }
export { encodeFrame, decodeFrame, packEntity, ENTITY_BYTES, headingToByte, byteToHeading, FRAME_DELTA, FRAME_KEYFRAME }

const MAGIC = 0x52 // 'R' — protocol version/sentinel; a mismatch means a desynced build
const HEADER_BYTES = 20
const ENTITY_BYTES = 15
const EVENT_BYTES = 10
const FRAME_DELTA = 0
const FRAME_KEYFRAME = 1

type SnapEntity = {
  id: number
  kind: number
  team: number
  x: number
  y: number
  hp: number
  maxHp: number
  heading: number // radians
  state: number
  progress: number // 0..1 (construction / production / harvest fill)
}

type SnapEvent = {
  type: number
  team: number
  x1: number
  y1: number
  x2: number
  y2: number
}

type Frame = {
  keyframe: boolean
  winner: number // 0 none, 1 red, 2 blue
  tick: number
  crystal: number
  supplyUsed: number
  supplyCap: number
  clockSec: number
  entities: SnapEntity[]
  removed: number[] // ids that left this audience's view (fog) or were destroyed
  events: SnapEvent[]
}

function headingToByte(rad: number): number {
  const twoPi = Math.PI * 2
  let n = rad % twoPi
  if (n < 0) n += twoPi
  return Math.round((n / twoPi) * 256) & 255
}

function byteToHeading(b: number): number {
  return (b / 256) * Math.PI * 2
}

function packEntity(view: DataView, off: number, e: SnapEntity): void {
  view.setUint16(off, e.id, true)
  view.setUint8(off + 2, e.kind)
  view.setUint8(off + 3, e.team)
  view.setInt16(off + 4, Math.round(e.x), true)
  view.setInt16(off + 6, Math.round(e.y), true)
  view.setUint16(off + 8, Math.max(0, Math.min(65535, Math.round(e.hp))), true)
  view.setUint16(off + 10, Math.max(0, Math.min(65535, Math.round(e.maxHp))), true)
  view.setUint8(off + 12, headingToByte(e.heading))
  view.setUint8(off + 13, e.state)
  view.setUint8(off + 14, Math.max(0, Math.min(255, Math.round(e.progress * 255))))
}

function unpackEntity(view: DataView, off: number): SnapEntity {
  return {
    id: view.getUint16(off, true),
    kind: view.getUint8(off + 2),
    team: view.getUint8(off + 3),
    x: view.getInt16(off + 4, true),
    y: view.getInt16(off + 6, true),
    hp: view.getUint16(off + 8, true),
    maxHp: view.getUint16(off + 10, true),
    heading: byteToHeading(view.getUint8(off + 12)),
    state: view.getUint8(off + 13),
    progress: view.getUint8(off + 14) / 255,
  }
}

function encodeFrame(f: Frame): Uint8Array {
  const total =
    HEADER_BYTES + f.entities.length * ENTITY_BYTES + 2 + f.removed.length * 2 + 1 + f.events.length * EVENT_BYTES
  const buf = new ArrayBuffer(total)
  const view = new DataView(buf)

  view.setUint8(0, MAGIC)
  view.setUint8(1, f.keyframe ? FRAME_KEYFRAME : FRAME_DELTA)
  view.setUint8(2, f.winner)
  view.setUint8(3, 0)
  view.setUint32(4, f.tick >>> 0, true)
  view.setUint32(8, Math.max(0, f.crystal) >>> 0, true)
  view.setUint16(12, Math.min(65535, f.supplyUsed), true)
  view.setUint16(14, Math.min(65535, f.supplyCap), true)
  view.setUint16(16, Math.min(65535, f.clockSec), true)
  view.setUint16(18, f.entities.length, true)

  let off = HEADER_BYTES
  for (const e of f.entities) {
    packEntity(view, off, e)
    off += ENTITY_BYTES
  }

  view.setUint16(off, f.removed.length, true)
  off += 2
  for (const id of f.removed) {
    view.setUint16(off, id, true)
    off += 2
  }

  const evCount = Math.min(255, f.events.length)
  view.setUint8(off, evCount)
  off += 1
  for (let i = 0; i < evCount; i++) {
    const ev = f.events[i]
    view.setUint8(off, ev.type)
    view.setUint8(off + 1, ev.team)
    view.setInt16(off + 2, Math.round(ev.x1), true)
    view.setInt16(off + 4, Math.round(ev.y1), true)
    view.setInt16(off + 6, Math.round(ev.x2), true)
    view.setInt16(off + 8, Math.round(ev.y2), true)
    off += EVENT_BYTES
  }

  return new Uint8Array(buf)
}

function decodeFrame(bytes: Uint8Array): Frame {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  if (view.getUint8(0) !== MAGIC) throw new Error('bad frame magic')
  const keyframe = view.getUint8(1) === FRAME_KEYFRAME
  const winner = view.getUint8(2)
  const tick = view.getUint32(4, true)
  const crystal = view.getUint32(8, true)
  const supplyUsed = view.getUint16(12, true)
  const supplyCap = view.getUint16(14, true)
  const clockSec = view.getUint16(16, true)
  const entityCount = view.getUint16(18, true)

  let off = HEADER_BYTES
  const entities: SnapEntity[] = new Array(entityCount)
  for (let i = 0; i < entityCount; i++) {
    entities[i] = unpackEntity(view, off)
    off += ENTITY_BYTES
  }

  const removedCount = view.getUint16(off, true)
  off += 2
  const removed: number[] = new Array(removedCount)
  for (let i = 0; i < removedCount; i++) {
    removed[i] = view.getUint16(off, true)
    off += 2
  }

  const eventCount = view.getUint8(off)
  off += 1
  const events: SnapEvent[] = new Array(eventCount)
  for (let i = 0; i < eventCount; i++) {
    events[i] = {
      type: view.getUint8(off),
      team: view.getUint8(off + 1),
      x1: view.getInt16(off + 2, true),
      y1: view.getInt16(off + 4, true),
      x2: view.getInt16(off + 6, true),
      y2: view.getInt16(off + 8, true),
    }
    off += EVENT_BYTES
  }

  return { keyframe, winner, tick, crystal, supplyUsed, supplyCap, clockSec, entities, removed, events }
}
