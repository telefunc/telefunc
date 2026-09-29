// The client's view of the world: decoded binary frames applied into an interpolated entity set.
//
// The authority broadcasts at 10 Hz; the renderer runs at display rate. This buffer keeps each
// entity's previous and current position and eases between them, so 10 Hz state reads as smooth
// 60fps motion. It also owns the keyframe/delta reconciliation the Room binary lane does not:
// a keyframe replaces the whole set; a delta merges changed entities and applies `removed`.

export type { RenderEntity }
export { WorldBuffer }

import { TICK_MS } from '../../shared/constants'
import type { Frame, SnapEntity, SnapEvent } from '../../shared/protocol'

type RenderEntity = {
  id: number
  kind: number
  team: number
  fromX: number
  fromY: number
  toX: number
  toY: number
  fromH: number
  toH: number
  segStart: number // ms timestamp when this from→to segment began
  hp: number
  maxHp: number
  state: number
  progress: number
  bornAt: number
}

type TimedEvent = SnapEvent & { at: number }

class WorldBuffer {
  entities = new Map<number, RenderEntity>()
  events: TimedEvent[] = []
  crystal = 0
  supplyUsed = 0
  supplyCap = 0
  clockSec = 0
  winner = 0
  tick = 0
  lastFrameAt = 0
  // ── finding #5 (keyframe-on-subscribe) — ✔ built as `{ retain: true }` (90dcb4a) ─────────────
  // Deltas can't be applied until a keyframe seeds the baseline. The authority marks each team-track
  // keyframe `{ retain: true }`, so the server replays the last one to a new subscriber before any
  // live frame — this client is seeded the instant it subscribes, with nothing to wire up. This gate
  // stays as a cheap safety net: ignore frames until the first keyframe arrives, so a delta that
  // races ahead of the retained replay can't corrupt an unseeded set. See server/sim/match.ts.
  private seeded = false

  reset(): void {
    this.entities.clear()
    this.events.length = 0
    this.seeded = false
    this.winner = 0
  }

  applyFrame(frame: Frame, now: number): void {
    if (!this.seeded && !frame.keyframe) return // wait for the seeding keyframe
    if (frame.keyframe) {
      this.seeded = true
      const incoming = new Set<number>()
      for (const e of frame.entities) {
        incoming.add(e.id)
        this.upsert(e, now)
      }
      for (const id of this.entities.keys()) if (!incoming.has(id)) this.entities.delete(id)
    } else {
      for (const e of frame.entities) this.upsert(e, now)
      for (const id of frame.removed) this.entities.delete(id)
    }

    for (const ev of frame.events) this.events.push({ ...ev, at: now })
    // Keep only recent events (particle lifetimes are short).
    if (this.events.length > 512) this.events.splice(0, this.events.length - 512)

    this.crystal = frame.crystal
    this.supplyUsed = frame.supplyUsed
    this.supplyCap = frame.supplyCap
    this.clockSec = frame.clockSec
    this.winner = frame.winner
    this.tick = frame.tick
    this.lastFrameAt = now
  }

  private upsert(e: SnapEntity, now: number): void {
    const cur = this.entities.get(e.id)
    if (cur) {
      // Continue motion from where we're currently drawing it.
      const a = Math.min(1, (now - cur.segStart) / TICK_MS)
      cur.fromX = cur.fromX + (cur.toX - cur.fromX) * a
      cur.fromY = cur.fromY + (cur.toY - cur.fromY) * a
      cur.fromH = cur.toH
      cur.toX = e.x
      cur.toY = e.y
      cur.toH = e.heading
      cur.segStart = now
      cur.hp = e.hp
      cur.maxHp = e.maxHp
      cur.state = e.state
      cur.progress = e.progress
      cur.kind = e.kind
      cur.team = e.team
    } else {
      this.entities.set(e.id, {
        id: e.id,
        kind: e.kind,
        team: e.team,
        fromX: e.x,
        fromY: e.y,
        toX: e.x,
        toY: e.y,
        fromH: e.heading,
        toH: e.heading,
        segStart: now,
        hp: e.hp,
        maxHp: e.maxHp,
        state: e.state,
        progress: e.progress,
        bornAt: now,
      })
    }
  }

  /** Interpolated position/heading for an entity at time `now`. */
  static sample(e: RenderEntity, now: number): { x: number; y: number; h: number } {
    const a = Math.min(1, (now - e.segStart) / TICK_MS)
    return {
      x: e.fromX + (e.toX - e.fromX) * a,
      y: e.fromY + (e.toY - e.fromY) * a,
      h: lerpAngle(e.fromH, e.toH, a),
    }
  }

  pruneEvents(now: number, lifetimeMs: number): void {
    let i = 0
    while (i < this.events.length && now - this.events[i].at > lifetimeMs) i++
    if (i > 0) this.events.splice(0, i)
  }
}

function lerpAngle(a: number, b: number, t: number): number {
  let d = b - a
  while (d > Math.PI) d -= Math.PI * 2
  while (d < -Math.PI) d += Math.PI * 2
  return a + d * t
}
