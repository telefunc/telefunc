export { applyCommand }

import { isBuildingKind, Kind, MAP_H, MAP_W, State, STATS, type Team } from '../../shared/constants'
import type { Command } from '../../shared/types'
import type { Entity, World } from './world'

// Validate and apply one player command against the authoritative world. Every decision here is
// the anti-cheat boundary: the caller's `team` is the server-verified `from.identity` mapped to a
// team (see server/sim/match.ts) — never trusted from the payload. Control is team-shared, so the
// gate is "does this entity belong to your team", not "did you personally build it".
function applyCommand(world: World, team: Team, cmd: Command): void {
  switch (cmd.t) {
    case 'move':
    case 'attackMove': {
      for (const e of ownedUnits(world, team, cmd.ids)) {
        clearOrders(e)
        e.tx = clamp(cmd.x, 0, MAP_W)
        e.ty = clamp(cmd.y, 0, MAP_H)
        e.attackMove = cmd.t === 'attackMove'
        e.state = State.MOVE
      }
      break
    }
    case 'attack': {
      const target = world.entities.get(cmd.target)
      if (!target || target.team === team) break
      for (const e of ownedUnits(world, team, cmd.ids)) {
        if (STATS[e.kind as Kind].damage <= 0) continue
        clearOrders(e)
        e.target = target.id
        e.state = State.ATTACK
      }
      break
    }
    case 'gather': {
      const node = world.entities.get(cmd.target)
      if (!node || node.kind !== Kind.CRYSTAL) break
      for (const e of ownedUnits(world, team, cmd.ids)) {
        if (e.kind !== Kind.WORKER) continue
        clearOrders(e)
        e.gatherNode = node.id
        e.state = State.MOVE
      }
      break
    }
    case 'stop': {
      for (const e of ownedUnits(world, team, cmd.ids)) {
        clearOrders(e)
        e.state = State.IDLE
      }
      break
    }
    case 'build': {
      const worker = world.entities.get(cmd.worker)
      if (!worker || worker.team !== team || worker.kind !== Kind.WORKER) break
      const s = STATS[cmd.building as Kind]
      if (!s || !STATS[Kind.WORKER].builds.includes(cmd.building as Kind)) break
      if ((world.crystal[team] ?? 0) < s.cost) break
      const x = clamp(cmd.x, s.radius, MAP_W - s.radius)
      const y = clamp(cmd.y, s.radius, MAP_H - s.radius)
      if (!placementClear(world, x, y, s.radius)) break
      world.crystal[team] -= s.cost
      const b = world.placeConstruction(cmd.building, team, x, y, worker.ownerId)
      clearOrders(worker)
      worker.buildTarget = b.id
      worker.state = State.MOVE
      break
    }
    case 'produce': {
      const b = world.entities.get(cmd.building)
      if (!b || b.team !== team || !isBuildingKind(b.kind) || b.state === State.CONSTRUCTING) break
      if (!STATS[b.kind as Kind].produces.includes(cmd.unit as Kind)) break
      const u = STATS[cmd.unit as Kind]
      if ((world.crystal[team] ?? 0) < u.cost) break
      if (world.supplyUsed(team) + pendingSupply(world, team) + u.supply > world.supplyCap(team)) break
      world.crystal[team] -= u.cost
      if (b.queue.length === 0) b.produceT = u.buildTime
      b.queue.push(cmd.unit)
      break
    }
    case 'cancel': {
      const b = world.entities.get(cmd.building)
      if (!b || b.team !== team || b.queue.length === 0) break
      const refunded = b.queue.pop() as number
      world.crystal[team] = (world.crystal[team] ?? 0) + STATS[refunded as Kind].cost
      if (b.queue.length === 0) b.produceT = 0
      else b.produceT = STATS[b.queue[0] as Kind].buildTime
      break
    }
    case 'rally': {
      const b = world.entities.get(cmd.building)
      if (!b || b.team !== team || !isBuildingKind(b.kind)) break
      b.rallyX = clamp(cmd.x, 0, MAP_W)
      b.rallyY = clamp(cmd.y, 0, MAP_H)
      break
    }
  }
}

function ownedUnits(world: World, team: Team, ids: number[]): Entity[] {
  const out: Entity[] = []
  for (const id of ids) {
    const e = world.entities.get(id)
    if (e && e.team === team && !isBuildingKind(e.kind) && e.kind !== Kind.CRYSTAL) out.push(e)
  }
  return out
}

function clearOrders(e: Entity): void {
  e.tx = null
  e.ty = null
  e.target = null
  e.gatherNode = null
  e.buildTarget = null
  e.attackMove = false
}

function placementClear(world: World, x: number, y: number, radius: number): boolean {
  for (const o of world.entities.values()) {
    if (!isBuildingKind(o.kind) && o.kind !== Kind.CRYSTAL) continue
    const min = STATS[o.kind as Kind].radius + radius + 12
    if (Math.hypot(o.x - x, o.y - y) < min) return false
  }
  return true
}

function pendingSupply(world: World, team: Team): number {
  let n = 0
  for (const e of world.entities.values()) {
    if (e.team === team && isBuildingKind(e.kind)) for (const q of e.queue) n += STATS[q as Kind].supply
  }
  return n
}

function clamp(v: number, lo: number, hi: number): number {
  return v < lo ? lo : v > hi ? hi : v
}
