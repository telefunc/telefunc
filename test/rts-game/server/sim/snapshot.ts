export { MatchSnapshots, TRACK }

import { HARVEST_TIME, isBuildingKind, Kind, RED, BLUE, State, STATS, type Team } from '../../shared/constants'
import { ENTITY_BYTES, packEntity, type Frame, type SnapEntity } from '../../shared/protocol'
import type { Entity, World } from './world'

// One named binary track per audience. The authority publishes each fog-filtered stream on its
// own track; a client subscribes to just its team's (see app/net.ts). `full` is the spectator
// stream — computed only while someone watches it (driven by `onDemand`, see match.ts).
//
// ── finding #6 (per-track vs per-subscriber interest) — working-as-designed ───────────────────
// `subscribeBinary({ track })` selectivity is by named track, enforced at the source — perfect for
// "2 teams = 2 fog streams" (an opponent's vision never touches your socket). It is deliberately
// *not* per-subscriber filtering: that would force a per-subscriber re-encode at the relay and
// destroy the property that bytes never leave the source. Per-player fog (or FFA) is honestly
// N tracks — one pre-computed + diffed baseline per audience, as below.
const TRACK = {
  red: 'state:red',
  blue: 'state:blue',
  full: 'state:full',
} as const
type TrackName = (typeof TRACK)[keyof typeof TRACK]

/** Delta compression is per track: the authority diffs each frame against the exact bytes that
 *  track last carried. A keyframe re-sends the whole visible set and rebases. */
class MatchSnapshots {
  private baseline = new Map<TrackName, Map<number, Uint8Array>>()
  private forced = new Set<TrackName>()

  constructor() {
    for (const t of Object.values(TRACK)) this.baseline.set(t, new Map())
  }

  /** Force the next frame on `track` to be a keyframe. Only the on-demand spectator `full` track
   *  needs this now: it's published solely while watched, so a fresh watcher may find no retained
   *  frame (or a stale one). The always-on team tracks instead mark keyframes `{ retain: true }`, so
   *  the server replays the last one to a new subscriber for free (see match.ts → `publish`). */
  forceKeyframe(track: TrackName): void {
    this.forced.add(track)
  }

  build(world: World, track: TrackName, periodic: boolean, clockSec: number): Frame {
    const team = track === TRACK.red ? RED : track === TRACK.blue ? BLUE : null // null = spectator (sees all)
    const keyframe = periodic || this.forced.has(track)
    this.forced.delete(track)
    const base = this.baseline.get(track) as Map<number, Uint8Array>

    const entities: SnapEntity[] = []
    const visibleIds = new Set<number>()
    const nextBase = new Map<number, Uint8Array>()

    for (const e of world.entities.values()) {
      if (team !== null && !world.visibleTo(e, team as Team)) continue
      visibleIds.add(e.id)
      const snap = toSnap(e)
      const packed = packOne(snap)
      nextBase.set(e.id, packed)
      if (keyframe) {
        entities.push(snap)
      } else {
        const prev = base.get(e.id)
        if (!prev || !bytesEqual(prev, packed)) entities.push(snap)
      }
    }

    // Removed = ids the baseline still had that are no longer visible (destroyed, or left fog).
    // Empty on a keyframe: the client replaces its whole set from `entities`.
    const removed: number[] = []
    if (!keyframe) for (const id of base.keys()) if (!visibleIds.has(id)) removed.push(id)

    this.baseline.set(track, nextBase)

    const events = team === null ? world.events : world.events.filter((ev) => visibleEvent(world, ev, team as Team))

    return {
      keyframe,
      winner: world.winner,
      tick: world.tickCount,
      crystal: team === null ? 0 : (world.crystal[team] ?? 0),
      supplyUsed: team === null ? 0 : world.supplyUsed(team as Team),
      supplyCap: team === null ? 0 : world.supplyCap(team as Team),
      clockSec,
      entities,
      removed,
      events,
    }
  }
}

function visibleEvent(world: World, ev: { x1: number; y1: number }, team: Team): boolean {
  // An event is shown to a team if its origin tile is currently lit for that team.
  return world.visibleTo({ x: ev.x1, y: ev.y1, team: 0 } as Entity, team)
}

function toSnap(e: Entity): SnapEntity {
  return {
    id: e.id,
    kind: e.kind,
    team: e.team,
    x: e.x,
    y: e.y,
    hp: e.hp,
    maxHp: e.maxHp,
    heading: e.heading,
    state: e.state,
    progress: progressOf(e),
  }
}

function progressOf(e: Entity): number {
  if (isBuildingKind(e.kind)) {
    if (e.state === State.CONSTRUCTING) return e.buildProg
    if (e.queue.length > 0) {
      const total = STATS[e.queue[0] as Kind].buildTime
      return total > 0 ? 1 - e.produceT / total : 0
    }
    return 0
  }
  if (e.kind === Kind.CRYSTAL) return e.maxHp > 0 ? e.hp / e.maxHp : 0
  if (e.kind === Kind.WORKER) return e.carrying > 0 ? 1 : Math.min(1, e.harvestT / HARVEST_TIME)
  return 0
}

const scratch = new DataView(new ArrayBuffer(ENTITY_BYTES))
function packOne(s: SnapEntity): Uint8Array {
  packEntity(scratch, 0, s)
  return new Uint8Array(scratch.buffer.slice(0))
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false
  return true
}
