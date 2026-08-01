// Pure, runtime-agnostic lane helpers shared by the Durable Object body and facade.

import type { LaneId } from '../../../../backend/spi.js'

// One collision-safe key indexes a lane's order, retained slot, routes, and delivery chain.
export { laneKey } from '../../../../backend/room/lane-key.js'

// Retained manifests preserve structural fields instead of reversing the encoded key.
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
