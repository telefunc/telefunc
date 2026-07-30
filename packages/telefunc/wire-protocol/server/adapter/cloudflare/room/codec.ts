// Pure, runtime-agnostic helpers shared by the Durable Object body and Node-side facade.
// Nothing here touches `cloudflare:workers`, SQL, or Node, so the exact same lane
// encoding and byte marshalling run on both sides of the RPC seam.

import type { LaneId } from '../../../../backend/spi.js'

// One key indexes a lane's order domain, its retained slot, its route rows and its delivery chain — the
// fixed lane table maps each lane's order domain and channel one to one. Member and track are encoded so
// a member named `a:b` can never collide with another lane.
export { laneKey } from '../../../../backend/subscription-source.js'

// Retained listing must hand back the exact LaneId objects the caller passed.
// The manifest row therefore carries the lane's structural fields verbatim
// rather than trying to reverse the encoded key.
export type LaneParts = { kind: LaneId['kind']; member: string | null; track: string | null }

export function laneToParts(lane: LaneId): LaneParts {
  switch (lane.kind) {
    case 'binary':
      return { kind: 'binary', member: lane.member, track: lane.track }
    case 'inbox':
      return { kind: 'inbox', member: lane.member, track: null }
    default:
      return { kind: lane.kind, member: null, track: null }
  }
}

export function partsToLane(parts: LaneParts): LaneId {
  switch (parts.kind) {
    case 'binary':
      return { kind: 'binary', member: parts.member as string, track: parts.track as string }
    case 'inbox':
      return { kind: 'inbox', member: parts.member as string }
    default:
      return { kind: parts.kind } as LaneId
  }
}

// Binary crosses the Node↔workerd RPC boundary as base64: a raw typed array survives Node→DO as an
// argument but a BLOB read back from SQLite does not round-trip cleanly as a DO→Node return value, so
// every binary the DO hands back is encoded and the facade decodes it. Kept dependency-free (no Buffer)
// so it runs identically in workerd and in Node.
const B64 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'
const B64_LOOKUP = /* #__PURE__ */ (() => {
  const table = new Int16Array(128).fill(-1)
  for (let i = 0; i < B64.length; i++) table[B64.charCodeAt(i)] = i
  return table
})()

export function bytesToBase64(bytes: Uint8Array): string {
  let out = ''
  const len = bytes.length
  for (let i = 0; i < len; i += 3) {
    const b0 = bytes[i] as number
    const b1 = i + 1 < len ? (bytes[i + 1] as number) : 0
    const b2 = i + 2 < len ? (bytes[i + 2] as number) : 0
    out += B64[b0 >> 2]
    out += B64[((b0 & 3) << 4) | (b1 >> 4)]
    out += i + 1 < len ? B64[((b1 & 15) << 2) | (b2 >> 6)] : '='
    out += i + 2 < len ? B64[b2 & 63] : '='
  }
  return out
}

export function base64ToBytes(b64: string): Uint8Array {
  let clean = 0
  for (let i = 0; i < b64.length; i++) if (b64.charCodeAt(i) !== 61) clean++ // 61 = '='
  const outLen = (clean * 3) >> 2
  const out = new Uint8Array(outLen)
  let acc = 0
  let bits = 0
  let o = 0
  for (let i = 0; i < b64.length; i++) {
    const code = b64.charCodeAt(i)
    if (code === 61) break
    const value = B64_LOOKUP[code] as number
    if (value < 0) continue
    acc = (acc << 6) | value
    bits += 6
    if (bits >= 8) {
      bits -= 8
      out[o++] = (acc >> bits) & 0xff
    }
  }
  return out
}
