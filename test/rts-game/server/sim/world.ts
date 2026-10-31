// The authoritative game world. This is the durable truth of a match — the Room holds only
// presence and the delivery lanes; nothing here lives inside the Room object.
//
// ── finding #7 (authoritative state / loop lifecycle) — working-as-designed ────────────────────
// A Room is presence + message lanes; it cannot *hold* an authoritative simulation, so the app
// owns this parallel world keyed by room id and keeps the two in sync by hand. That's by design:
// a per-room authoritative loop in a multi-node deployment needs leader election, which the Room
// can't provide — single-node, the `globalThis` singleton registry + the server seat's `onEmpty`
// (server/matches.ts) are the idiomatic shape, documented in 51b4613. See README finding #7.
// ──────────────────────────────────────────────────────────────────────────────────────────────

export type { Entity }
export { World }

import {
  BLUE,
  CRYSTAL_AMOUNT,
  HARVEST_TIME,
  isBuildingKind,
  Kind,
  MAP_H,
  MAP_W,
  NEUTRAL,
  RED,
  START_CRYSTAL,
  State,
  STATS,
  type Team,
} from '../../shared/constants'
import type { SnapEvent } from '../../shared/protocol'
import { Ev } from '../../shared/constants'

type Entity = {
  id: number
  kind: number
  team: Team
  x: number
  y: number
  hp: number
  maxHp: number
  heading: number
  state: number
  cd: number // attack cooldown remaining (s)
  // movement / orders
  tx: number | null
  ty: number | null
  attackMove: boolean
  target: number | null // explicit attack target entity id
  // worker economy
  carrying: number
  harvestT: number
  gatherNode: number | null
  buildTarget: number | null // building (id) this worker is constructing
  // building
  queue: number[] // unit kinds queued to produce
  produceT: number // seconds remaining on the front queue item
  rallyX: number | null
  rallyY: number | null
  buildProg: number // 0..1 for under-construction buildings
  buildTotal: number // seconds of construction
  ownerId: string // identity that created it (informational; control is team-shared)
}

const FOG_TILE = 80
const FOG_W = Math.ceil(MAP_W / FOG_TILE)
const FOG_H = Math.ceil(MAP_H / FOG_TILE)

class World {
  entities = new Map<number, Entity>()
  crystal: Record<number, number> = { [RED]: START_CRYSTAL, [BLUE]: START_CRYSTAL }
  teams = new Set<Team>()
  winner: Team = NEUTRAL
  tickCount = 0
  events: SnapEvent[] = [] // ephemeral visuals produced this tick, drained by the snapshot builder
  private nextId = 1
  private fog: Record<number, Uint8Array> = {
    [RED]: new Uint8Array(FOG_W * FOG_H),
    [BLUE]: new Uint8Array(FOG_W * FOG_H),
  }

  spawn(kind: number, team: Team, x: number, y: number, ownerId = ''): Entity {
    const s = STATS[kind as Kind]
    const e: Entity = {
      id: this.nextId++,
      kind,
      team,
      x,
      y,
      hp: s.hp,
      maxHp: s.hp,
      heading: team === BLUE ? Math.PI : 0,
      state: State.IDLE,
      cd: 0,
      tx: null,
      ty: null,
      attackMove: false,
      target: null,
      carrying: 0,
      harvestT: 0,
      gatherNode: null,
      buildTarget: null,
      queue: [],
      produceT: 0,
      rallyX: null,
      rallyY: null,
      buildProg: 1,
      buildTotal: 0,
      ownerId,
    }
    if (kind === Kind.CRYSTAL) {
      e.hp = CRYSTAL_AMOUNT
      e.maxHp = CRYSTAL_AMOUNT
      e.team = NEUTRAL
    }
    this.entities.set(e.id, e)
    return e
  }

  /** Begin a building's construction: it exists immediately (blocks, targetable) but at low HP
   *  and 0 progress; a worker in BUILD state adjacent advances it. */
  placeConstruction(kind: number, team: Team, x: number, y: number, ownerId: string): Entity {
    const e = this.spawn(kind, team, x, y, ownerId)
    const s = STATS[kind as Kind]
    e.state = State.CONSTRUCTING
    e.buildProg = 0
    e.buildTotal = s.buildTime
    e.hp = Math.max(1, Math.round(s.hp * 0.1))
    return e
  }

  supplyUsed(team: Team): number {
    let n = 0
    for (const e of this.entities.values())
      if (e.team === team && !isBuildingKind(e.kind)) n += STATS[e.kind as Kind].supply
    return n
  }

  supplyCap(team: Team): number {
    let n = 0
    for (const e of this.entities.values())
      if (e.team === team && isBuildingKind(e.kind) && e.state !== State.CONSTRUCTING)
        n += STATS[e.kind as Kind].supplyProvided
    return Math.min(200, n)
  }

  buildingCount(team: Team): number {
    let n = 0
    for (const e of this.entities.values()) if (e.team === team && isBuildingKind(e.kind)) n++
    return n
  }

  tick(dt: number): void {
    this.tickCount++
    this.events = []
    this.computeFog()

    for (const e of this.entities.values()) {
      if (isBuildingKind(e.kind)) this.tickBuilding(e, dt)
      else if (e.kind !== Kind.CRYSTAL) this.tickUnit(e, dt)
    }

    this.resolveCollisions()
    this.checkWin()
  }

  // ── fog of war (team-shared vision) ───────────────────────────────────────────────────────
  private computeFog(): void {
    this.fog[RED].fill(0)
    this.fog[BLUE].fill(0)
    for (const e of this.entities.values()) {
      if (e.team !== RED && e.team !== BLUE) continue
      const sight = STATS[e.kind as Kind].sight
      if (sight <= 0) continue
      this.lightTiles(this.fog[e.team], e.x, e.y, sight)
    }
  }

  private lightTiles(grid: Uint8Array, x: number, y: number, r: number): void {
    const minTx = Math.max(0, Math.floor((x - r) / FOG_TILE))
    const maxTx = Math.min(FOG_W - 1, Math.floor((x + r) / FOG_TILE))
    const minTy = Math.max(0, Math.floor((y - r) / FOG_TILE))
    const maxTy = Math.min(FOG_H - 1, Math.floor((y + r) / FOG_TILE))
    const r2 = r * r
    for (let ty = minTy; ty <= maxTy; ty++) {
      for (let tx = minTx; tx <= maxTx; tx++) {
        const cx = tx * FOG_TILE + FOG_TILE / 2
        const cy = ty * FOG_TILE + FOG_TILE / 2
        const dx = cx - x
        const dy = cy - y
        if (dx * dx + dy * dy <= r2) grid[ty * FOG_W + tx] = 1
      }
    }
  }

  /** Is `e` inside `team`'s current vision? Used by the snapshot builder to filter enemies. */
  visibleTo(e: Entity, team: Team): boolean {
    if (e.team === team) return true
    const tx = Math.min(FOG_W - 1, Math.max(0, Math.floor(e.x / FOG_TILE)))
    const ty = Math.min(FOG_H - 1, Math.max(0, Math.floor(e.y / FOG_TILE)))
    return this.fog[team][ty * FOG_W + tx] === 1
  }

  // ── buildings: production queue + construction ────────────────────────────────────────────
  private tickBuilding(e: Entity, dt: number): void {
    if (e.state === State.CONSTRUCTING) {
      // Progress advances only while at least one worker is in BUILD state adjacent to it.
      if (this.hasBuilderAt(e)) {
        e.buildProg = Math.min(1, e.buildProg + dt / Math.max(0.1, e.buildTotal))
        e.hp = Math.max(1, Math.round(STATS[e.kind as Kind].hp * (0.1 + 0.9 * e.buildProg)))
        if (e.buildProg >= 1) {
          e.state = State.IDLE
          e.hp = e.maxHp
          this.events.push({ type: Ev.COMPLETE, team: e.team, x1: e.x, y1: e.y, x2: e.x, y2: e.y })
        }
      }
      return
    }
    // Production queue: advance the front item; spawn on completion.
    if (e.queue.length > 0) {
      e.produceT -= dt
      e.state = State.IDLE
      if (e.produceT <= 0) {
        const unit = e.queue.shift() as number
        const spawn = this.exitPoint(e)
        const u = this.spawn(unit, e.team, spawn.x, spawn.y, e.ownerId)
        // Send the fresh unit to the rally point (or just clear of the building).
        if (e.rallyX !== null && e.rallyY !== null) {
          u.tx = e.rallyX
          u.ty = e.rallyY
          u.state = State.MOVE
        }
        e.produceT = e.queue.length > 0 ? STATS[e.queue[0] as Kind].buildTime : 0
      }
    }
  }

  private hasBuilderAt(b: Entity): boolean {
    for (const e of this.entities.values()) {
      if (e.kind === Kind.WORKER && e.buildTarget === b.id && e.state === State.BUILD) {
        if (dist(e, b) <= STATS[b.kind as Kind].radius + 40) return true
      }
    }
    return false
  }

  private exitPoint(b: Entity): { x: number; y: number } {
    const r = STATS[b.kind as Kind].radius + 26
    const ang = b.team === BLUE ? Math.PI * 1.25 : Math.PI * 0.25
    return {
      x: clamp(b.x + Math.cos(ang) * r, 20, MAP_W - 20),
      y: clamp(b.y + Math.sin(ang) * r, 20, MAP_H - 20),
    }
  }

  // ── units: movement, economy, combat ──────────────────────────────────────────────────────
  private tickUnit(e: Entity, dt: number): void {
    if (e.cd > 0) e.cd -= dt
    const s = STATS[e.kind as Kind]

    // Worker economy loop takes priority over auto-combat.
    if (e.kind === Kind.WORKER && (e.gatherNode !== null || e.carrying > 0)) {
      this.tickHarvest(e, dt, s)
      return
    }
    if (e.kind === Kind.WORKER && e.buildTarget !== null) {
      this.tickBuild(e, dt, s)
      return
    }

    // Combat: honor an explicit target, else auto-acquire when idle or attack-moving.
    let target = e.target !== null ? this.entities.get(e.target) : undefined
    if (target && (target.hp <= 0 || !this.canAttack(e, target))) target = undefined
    if (!target && s.damage > 0 && (e.state === State.IDLE || e.attackMove || e.target !== null)) {
      target = this.acquireTarget(e, s)
    }

    if (target) {
      const d = dist(e, target)
      const reach = s.range + s.radius + STATS[target.kind as Kind].radius
      e.heading = Math.atan2(target.y - e.y, target.x - e.x)
      if (d <= reach) {
        e.state = State.ATTACK
        e.tx = null
        e.ty = null
        if (e.cd <= 0) this.fire(e, target, s)
      } else {
        // Move into range.
        this.moveToward(e, target.x, target.y, dt, s)
        e.state = State.MOVE
      }
      return
    }

    // Plain move order.
    if (e.tx !== null && e.ty !== null) {
      const arrived = this.moveToward(e, e.tx, e.ty, dt, s)
      e.state = State.MOVE
      if (arrived) {
        e.tx = null
        e.ty = null
        e.attackMove = false
        e.state = State.IDLE
      }
    } else {
      e.state = State.IDLE
    }
  }

  private canAttack(e: Entity, target: Entity): boolean {
    return target.team !== NEUTRAL && target.team !== e.team
  }

  private acquireTarget(e: Entity, s: (typeof STATS)[Kind]): Entity | undefined {
    // Aggro within sight; when attack-moving, chase; when idle-defending, a shorter leash.
    const range = e.attackMove ? s.sight : Math.max(s.range + 60, s.sight * 0.6)
    let best: Entity | undefined
    let bestD = range
    for (const o of this.entities.values()) {
      if (!this.canAttack(e, o) || o.hp <= 0) continue
      const d = dist(e, o)
      if (d < bestD) {
        bestD = d
        best = o
      }
    }
    return best
  }

  private fire(e: Entity, target: Entity, s: (typeof STATS)[Kind]): void {
    e.cd = s.cooldown
    target.hp -= s.damage
    this.events.push({ type: Ev.SHOT, team: e.team, x1: e.x, y1: e.y, x2: target.x, y2: target.y })
    if (target.hp <= 0) {
      this.events.push({
        type: Ev.EXPLOSION,
        team: target.team,
        x1: target.x,
        y1: target.y,
        x2: target.x,
        y2: target.y,
      })
      this.destroy(target)
    }
  }

  private destroy(target: Entity): void {
    this.entities.delete(target.id)
    // Any worker mid-build on a destroyed site, or units targeting it, fall idle naturally
    // (their target/buildTarget id no longer resolves).
  }

  // ── worker: harvest cycle ─────────────────────────────────────────────────────────────────
  private tickHarvest(e: Entity, dt: number, s: (typeof STATS)[Kind]): void {
    if (e.carrying > 0) {
      // Returning to the nearest dropoff.
      const drop = this.nearestDropoff(e)
      if (!drop) {
        e.state = State.IDLE
        return
      }
      e.state = State.RETURN
      if (this.moveToward(e, drop.x, drop.y, dt, s, STATS[drop.kind as Kind].radius + 18)) {
        this.crystal[e.team] = (this.crystal[e.team] ?? 0) + e.carrying
        e.carrying = 0
        // Head back to the node if it still exists.
        const node = e.gatherNode !== null ? this.entities.get(e.gatherNode) : undefined
        if (!node) e.gatherNode = this.findCrystalNear(e)?.id ?? null
      }
      return
    }

    const node = e.gatherNode !== null ? this.entities.get(e.gatherNode) : undefined
    if (!node) {
      const next = this.findCrystalNear(e)
      e.gatherNode = next?.id ?? null
      if (!next) {
        e.state = State.IDLE
        return
      }
      return
    }
    // Move to node, then mine.
    if (dist(e, node) > STATS[Kind.CRYSTAL].radius + s.radius + 8) {
      e.state = State.MOVE
      this.moveToward(e, node.x, node.y, dt, s, STATS[Kind.CRYSTAL].radius + s.radius + 4)
      return
    }
    e.state = State.GATHER
    e.harvestT += dt
    e.heading = Math.atan2(node.y - e.y, node.x - e.x)
    if (e.harvestT >= HARVEST_TIME) {
      e.harvestT = 0
      const take = Math.min(s.carry, node.hp)
      node.hp -= take
      e.carrying = take
      if (node.hp <= 0) this.entities.delete(node.id)
    }
  }

  private tickBuild(e: Entity, dt: number, s: (typeof STATS)[Kind]): void {
    const b = e.buildTarget !== null ? this.entities.get(e.buildTarget) : undefined
    if (!b || b.state !== State.CONSTRUCTING) {
      e.buildTarget = null
      e.state = State.IDLE
      return
    }
    const reach = STATS[b.kind as Kind].radius + s.radius + 10
    if (dist(e, b) > reach) {
      e.state = State.MOVE
      this.moveToward(e, b.x, b.y, dt, s, reach - 4)
    } else {
      e.state = State.BUILD
      e.heading = Math.atan2(b.y - e.y, b.x - e.x)
    }
  }

  private nearestDropoff(e: Entity): Entity | undefined {
    let best: Entity | undefined
    let bestD = Infinity
    for (const o of this.entities.values()) {
      if (o.team === e.team && STATS[o.kind as Kind].dropoff && o.state !== State.CONSTRUCTING) {
        const d = dist(e, o)
        if (d < bestD) {
          bestD = d
          best = o
        }
      }
    }
    return best
  }

  private findCrystalNear(e: Entity): Entity | undefined {
    let best: Entity | undefined
    let bestD = Infinity
    for (const o of this.entities.values()) {
      if (o.kind === Kind.CRYSTAL) {
        const d = dist(e, o)
        if (d < bestD) {
          bestD = d
          best = o
        }
      }
    }
    return best
  }

  // ── steering ──────────────────────────────────────────────────────────────────────────────
  /** Move `e` toward (x,y). Returns true on arrival (within `stop`). Applies light obstacle
   *  avoidance around buildings/crystals so units don't wedge on them. */
  private moveToward(e: Entity, x: number, y: number, dt: number, s: (typeof STATS)[Kind], stop = 6): boolean {
    let dx = x - e.x
    let dy = y - e.y
    const d = Math.hypot(dx, dy)
    if (d <= stop) return true
    dx /= d
    dy /= d
    // Avoid static obstacles (buildings, crystals) in the path.
    for (const o of this.entities.values()) {
      if (o === e) continue
      if (!isBuildingKind(o.kind) && o.kind !== Kind.CRYSTAL) continue
      if (o.id === e.buildTarget || o.id === e.gatherNode) continue
      const or = STATS[o.kind as Kind].radius + s.radius + 6
      const ox = o.x - e.x
      const oy = o.y - e.y
      const od = Math.hypot(ox, oy)
      if (od < or && od > 0.001) {
        // Steer tangentially around the obstacle.
        const push = (or - od) / or
        dx += (-ox / od) * push * 1.4
        dy += (-oy / od) * push * 1.4
      }
    }
    const len = Math.hypot(dx, dy) || 1
    dx /= len
    dy /= len
    const step = s.speed * dt
    e.x = clamp(e.x + dx * step, s.radius, MAP_W - s.radius)
    e.y = clamp(e.y + dy * step, s.radius, MAP_H - s.radius)
    e.heading = Math.atan2(dy, dx)
    return Math.hypot(x - e.x, y - e.y) <= stop
  }

  /** Soft separation so mobile units don't fully stack, plus hard ejection out of buildings. */
  private resolveCollisions(): void {
    const units: Entity[] = []
    for (const e of this.entities.values()) if (!isBuildingKind(e.kind) && e.kind !== Kind.CRYSTAL) units.push(e)
    for (let i = 0; i < units.length; i++) {
      const a = units[i]
      const ar = STATS[a.kind as Kind].radius
      for (let j = i + 1; j < units.length; j++) {
        const b = units[j]
        const br = STATS[b.kind as Kind].radius
        const dx = b.x - a.x
        const dy = b.y - a.y
        const d = Math.hypot(dx, dy)
        const min = ar + br
        if (d < min && d > 0.001) {
          const push = (min - d) / 2
          const nx = dx / d
          const ny = dy / d
          a.x = clamp(a.x - nx * push, ar, MAP_W - ar)
          a.y = clamp(a.y - ny * push, ar, MAP_H - ar)
          b.x = clamp(b.x + nx * push, br, MAP_W - br)
          b.y = clamp(b.y + ny * push, br, MAP_H - br)
        }
      }
      // Eject from buildings/crystals.
      for (const o of this.entities.values()) {
        if (!isBuildingKind(o.kind) && o.kind !== Kind.CRYSTAL) continue
        const min = STATS[o.kind as Kind].radius + ar
        const dx = a.x - o.x
        const dy = a.y - o.y
        const d = Math.hypot(dx, dy)
        if (d < min && d > 0.001) {
          a.x = clamp(o.x + (dx / d) * min, ar, MAP_W - ar)
          a.y = clamp(o.y + (dy / d) * min, ar, MAP_H - ar)
        }
      }
    }
  }

  private checkWin(): void {
    if (this.winner !== NEUTRAL) return
    const alive: Team[] = []
    for (const t of this.teams) if (this.buildingCount(t) > 0) alive.push(t)
    if (this.teams.size >= 1 && alive.length <= 1) {
      this.winner = alive[0] ?? NEUTRAL
    }
  }

  /** A team resigns (or its last player leaves the match): wipe it and let win detection settle. */
  eliminate(team: Team): void {
    for (const e of [...this.entities.values()]) {
      if (e.team === team) {
        if (isBuildingKind(e.kind)) this.events.push({ type: Ev.EXPLOSION, team, x1: e.x, y1: e.y, x2: e.x, y2: e.y })
        this.entities.delete(e.id)
      }
    }
    this.checkWin()
  }
}

function dist(a: { x: number; y: number }, b: { x: number; y: number }): number {
  return Math.hypot(a.x - b.x, a.y - b.y)
}

function clamp(v: number, lo: number, hi: number): number {
  return v < lo ? lo : v > hi ? hi : v
}
