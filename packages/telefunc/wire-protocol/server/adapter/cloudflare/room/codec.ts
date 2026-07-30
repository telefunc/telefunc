// Pure, runtime-agnostic lane helpers shared by the Durable Object body and facade.

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
