export { seedWorld }

import { BLUE, Kind, MAP_H, MAP_W, RED, State, type Team } from '../../shared/constants'
import type { World } from './world'

const BASE_MARGIN = 440

/** Seed a fresh match world: one HQ + a worker line per player (bases spread along the team's
 *  back line), crystal fields by each base, and a contested center field. Starting workers
 *  auto-assign to the nearest node so the economy is live from tick 0. */
function seedWorld(world: World, teamPlayers: Map<Team, string[]>): void {
  const center = { x: MAP_W / 2, y: MAP_H / 2 }
  const anchors: Record<number, { x: number; y: number }> = {
    [RED]: { x: BASE_MARGIN, y: BASE_MARGIN },
    [BLUE]: { x: MAP_W - BASE_MARGIN, y: MAP_H - BASE_MARGIN },
  }

  for (const [team, players] of teamPlayers) {
    if (players.length === 0) continue
    world.teams.add(team)
    const anchor = anchors[team]
    // Perpendicular to the base→center axis, to fan multiple players' bases apart.
    const toC = norm(center.x - anchor.x, center.y - anchor.y)
    const perp = { x: -toC.y, y: toC.x }
    const n = players.length
    players.forEach((pid, i) => {
      const spread = (i - (n - 1) / 2) * 260
      const bx = clamp(anchor.x + perp.x * spread, 120, MAP_W - 120)
      const by = clamp(anchor.y + perp.y * spread, 120, MAP_H - 120)
      const hq = world.spawn(Kind.HQ, team, bx, by, pid)

      // A crystal field just behind each base.
      const c1 = world.spawn(
        Kind.CRYSTAL,
        team,
        clamp(bx - toC.x * 40 + perp.x * 150, 60, MAP_W - 60),
        clamp(by - toC.y * 40 + perp.y * 150, 60, MAP_H - 60),
      )
      const c2 = world.spawn(
        Kind.CRYSTAL,
        team,
        clamp(bx + toC.x * 150 - perp.x * 40, 60, MAP_W - 60),
        clamp(by + toC.y * 150 - perp.y * 40, 60, MAP_H - 60),
      )

      // Five workers, fanned in front of the HQ, auto-gathering.
      for (let w = 0; w < 5; w++) {
        const ang = Math.atan2(toC.y, toC.x) + (w - 2) * 0.32
        const wx = clamp(bx + Math.cos(ang) * 100, 40, MAP_W - 40)
        const wy = clamp(by + Math.sin(ang) * 100, 40, MAP_H - 40)
        const worker = world.spawn(Kind.WORKER, team, wx, wy, pid)
        worker.gatherNode = (w % 2 === 0 ? c1 : c2).id
        worker.state = State.MOVE
      }
    })
  }

  // Contested center crystal field.
  world.spawn(Kind.CRYSTAL, 0 as Team, center.x - 120, center.y - 120)
  world.spawn(Kind.CRYSTAL, 0 as Team, center.x + 120, center.y + 120)
  world.spawn(Kind.CRYSTAL, 0 as Team, center.x + 140, center.y - 140)
}

function norm(x: number, y: number): { x: number; y: number } {
  const d = Math.hypot(x, y) || 1
  return { x: x / d, y: y / d }
}

function clamp(v: number, lo: number, hi: number): number {
  return v < lo ? lo : v > hi ? hi : v
}
