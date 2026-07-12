export { SceneRenderer }
export type { FrameParams }

import { Application, Container, Graphics, Sprite, Texture } from 'pixi.js'
import {
  BLUE,
  Ev,
  isBuildingKind,
  Kind,
  MAP_H,
  MAP_W,
  NEUTRAL,
  RED,
  State,
  STATS,
  TEAM_COLOR,
  type Team,
} from '../../shared/constants'
import type { Camera } from './camera'
import type { Fog } from './fog'
import { WorldBuffer, type RenderEntity } from './world-buffer'

type FrameParams = {
  now: number
  camera: Camera
  buffer: WorldBuffer
  fog: Fog
  myTeam: Team
  selection: Set<number>
  box: { x0: number; y0: number; x1: number; y1: number } | null
  markers: { x: number; y: number; at: number; kind: 'move' | 'attack' | 'rally' }[]
  placement: { kind: number; x: number; y: number; valid: boolean } | null
}

const EVENT_LIFE = 380

/** One pooled display object per entity. Body geometry is drawn once (per kind + team); each frame
 *  only cheap transform / bar / ring updates run, so hundreds of entities stay on the GPU's happy
 *  path. */
class EntitySprite {
  root = new Container()
  private bodyRot = new Container()
  private hpBg = new Graphics()
  private hpFg = new Graphics()
  private ring = new Graphics()
  private prog = new Graphics()
  private lastFrac = -1
  private lastProg = -1
  readonly radius: number

  constructor(e: RenderEntity) {
    const s = STATS[e.kind as Kind]
    this.radius = s?.radius ?? 12
    drawBody(this.bodyRot, e.kind, e.team)
    const bw = Math.max(20, this.radius * 2)
    this.hpBg.rect(-bw / 2, 0, bw, 5).fill({ color: 0x0a0e17, alpha: 0.9 })
    this.hpBg.rect(-bw / 2, 0, bw, 5).stroke({ color: 0x000000, alpha: 0.5, width: 1 })
    this.hpFg.rect(0, 0, bw, 5).fill(0x62e07a)
    this.hpFg.x = -bw / 2
    const barY = -this.radius - 12
    this.hpBg.y = barY
    this.hpFg.y = barY
    this.ring.circle(0, 0, this.radius + 4).stroke({ color: 0x8be08a, width: 2, alpha: 0.95 })
    this.ring.visible = false
    this.hpBg.visible = false
    this.hpFg.visible = false
    this.root.addChild(this.ring, this.bodyRot, this.prog, this.hpBg, this.hpFg)
  }

  update(e: RenderEntity, x: number, y: number, h: number, selected: boolean): void {
    this.root.x = x
    this.root.y = y
    if (!isBuildingKind(e.kind) && e.kind !== Kind.CRYSTAL) this.bodyRot.rotation = h
    this.ring.visible = selected

    const frac = e.maxHp > 0 ? Math.max(0, Math.min(1, e.hp / e.maxHp)) : 1
    const showHp = e.kind !== Kind.CRYSTAL && (frac < 0.999 || selected)
    this.hpBg.visible = showHp
    this.hpFg.visible = showHp
    if (showHp && Math.abs(frac - this.lastFrac) > 0.004) {
      this.hpFg.scale.x = frac
      this.hpFg.tint = frac > 0.55 ? 0x62e07a : frac > 0.28 ? 0xffcf5c : 0xff5a5a
      this.lastFrac = frac
    }

    // Construction / production / harvest progress ring under buildings & workers.
    const showProg =
      (isBuildingKind(e.kind) && (e.state === State.CONSTRUCTING || e.progress > 0.001)) ||
      (e.kind === Kind.CRYSTAL && e.progress < 0.999)
    if (showProg && Math.abs(e.progress - this.lastProg) > 0.01) {
      this.prog.clear()
      if (e.kind === Kind.CRYSTAL) {
        // Depletion: dim the crystal as it empties (handled via alpha below).
      } else {
        const r = this.radius + 8
        this.prog.circle(0, 0, r).stroke({ color: 0x1e2740, width: 3, alpha: 0.8 })
        this.prog
          .arc(0, 0, r, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * e.progress)
          .stroke({ color: e.state === State.CONSTRUCTING ? 0xffcf5c : 0x7cc4ff, width: 3 })
      }
      this.lastProg = e.progress
    }
    this.prog.visible = showProg && e.kind !== Kind.CRYSTAL
    this.bodyRot.alpha = e.state === State.CONSTRUCTING ? 0.55 : e.kind === Kind.CRYSTAL ? 0.55 + 0.45 * e.progress : 1
  }

  destroy(): void {
    this.root.destroy({ children: true })
  }
}

class SceneRenderer {
  app = new Application()
  private world = new Container()
  private terrain = new Container()
  private entityLayer = new Container()
  private effects = new Graphics()
  private overlay = new Graphics() // screen-space (selection box)
  private fogSprite: Sprite | null = null
  private fogTex: Texture | null = null
  private sprites = new Map<number, EntitySprite>()
  private lastFogAt = 0
  private inited = false
  private disposed = false

  async init(host: HTMLElement): Promise<void> {
    await this.app.init({
      resizeTo: host,
      background: 0x070b13,
      antialias: true,
      resolution: Math.min(2, window.devicePixelRatio || 1),
      autoDensity: true,
      preference: 'webgl',
    })
    if (this.disposed) {
      this.app.destroy(true)
      return
    }
    host.appendChild(this.app.canvas)
    this.app.canvas.style.display = 'block'
    this.world.addChild(this.terrain, this.entityLayer, this.effects)
    this.app.stage.addChild(this.world, this.overlay)
    this.drawTerrain()
    this.inited = true
  }

  attachFog(fog: Fog): void {
    this.fogTex = Texture.from(fog.canvas)
    this.fogTex.source.scaleMode = 'linear'
    this.fogSprite = new Sprite(this.fogTex)
    this.fogSprite.width = MAP_W
    this.fogSprite.height = MAP_H
    this.world.addChild(this.fogSprite) // above entities: darkens unexplored terrain
  }

  private drawTerrain(): void {
    const g = new Graphics()
    g.rect(0, 0, MAP_W, MAP_H).fill(0x0c1220)
    // Subtle grid.
    for (let x = 0; x <= MAP_W; x += 200) g.moveTo(x, 0).lineTo(x, MAP_H)
    for (let y = 0; y <= MAP_H; y += 200) g.moveTo(0, y).lineTo(MAP_W, y)
    g.stroke({ color: 0x131c30, width: 2 })
    // Base glow markers at the two corners.
    g.circle(440, 440, 260).fill({ color: TEAM_COLOR[RED], alpha: 0.05 })
    g.circle(MAP_W - 440, MAP_H - 440, 260).fill({ color: TEAM_COLOR[BLUE], alpha: 0.05 })
    g.rect(0, 0, MAP_W, MAP_H).stroke({ color: 0x2c3856, width: 6 })
    this.terrain.addChild(g)
  }

  frame(p: FrameParams): void {
    if (!this.inited || this.disposed) return
    p.camera.applyTo(this.world)

    // Fog recompute is throttled to the sim rate; the texture is tiny (1px/tile).
    if (p.now - this.lastFogAt > 90) {
      p.fog.update(p.buffer.entities.values(), p.myTeam, p.now)
      this.fogTex?.source.update()
      this.lastFogAt = p.now
    }

    const seen = new Set<number>()
    for (const e of p.buffer.entities.values()) {
      seen.add(e.id)
      let sp = this.sprites.get(e.id)
      if (!sp) {
        sp = new EntitySprite(e)
        this.sprites.set(e.id, sp)
        this.entityLayer.addChild(sp.root)
      }
      const s = WorldBuffer.sample(e, p.now)
      sp.update(e, s.x, s.y, s.h, p.selection.has(e.id))
    }
    for (const [id, sp] of this.sprites) {
      if (!seen.has(id)) {
        sp.destroy()
        this.sprites.delete(id)
      }
    }

    this.drawEffects(p)
    this.drawOverlay(p)
  }

  private drawEffects(p: FrameParams): void {
    const g = this.effects
    g.clear()
    p.buffer.pruneEvents(p.now, EVENT_LIFE)
    for (const ev of p.buffer.events) {
      const age = (p.now - ev.at) / EVENT_LIFE
      if (age > 1) continue
      const col = ev.team === RED ? TEAM_COLOR[RED] : ev.team === BLUE ? TEAM_COLOR[BLUE] : 0xffffff
      if (ev.type === Ev.SHOT) {
        g.moveTo(ev.x1, ev.y1)
          .lineTo(ev.x2, ev.y2)
          .stroke({ color: 0xfff2b0, width: 2, alpha: 1 - age })
      } else if (ev.type === Ev.EXPLOSION) {
        g.circle(ev.x1, ev.y1, 10 + age * 46).stroke({ color: 0xff8a5c, width: 3, alpha: 1 - age })
      } else if (ev.type === Ev.COMPLETE) {
        g.circle(ev.x1, ev.y1, 30 + age * 40).stroke({ color: col, width: 3, alpha: 1 - age })
      }
    }
    // Order markers (client feedback).
    for (const m of p.markers) {
      const age = (p.now - m.at) / 520
      if (age > 1) continue
      const col = m.kind === 'attack' ? 0xff5a5a : m.kind === 'rally' ? 0xffcf5c : 0x8be08a
      g.circle(m.x, m.y, 22 * (1 - age) + 4).stroke({ color: col, width: 2, alpha: 1 - age })
    }
    // Build placement ghost.
    if (p.placement) {
      const r = STATS[p.placement.kind as Kind]?.radius ?? 40
      g.circle(p.placement.x, p.placement.y, r)
        .fill({ color: p.placement.valid ? 0x52e0c4 : 0xff5a5a, alpha: 0.18 })
        .stroke({ color: p.placement.valid ? 0x52e0c4 : 0xff5a5a, width: 2 })
    }
  }

  private drawOverlay(p: FrameParams): void {
    const g = this.overlay
    g.clear()
    if (p.box) {
      const x = Math.min(p.box.x0, p.box.x1)
      const y = Math.min(p.box.y0, p.box.y1)
      const w = Math.abs(p.box.x1 - p.box.x0)
      const h = Math.abs(p.box.y1 - p.box.y0)
      g.rect(x, y, w, h).fill({ color: 0x8be08a, alpha: 0.1 }).stroke({ color: 0x8be08a, width: 1.5 })
    }
  }

  dispose(): void {
    if (this.disposed) return
    this.disposed = true
    if (!this.inited) return // init() will tear down the half-built app itself
    for (const sp of this.sprites.values()) sp.destroy()
    this.sprites.clear()
    this.fogTex?.destroy()
    this.app.destroy(true, { children: true, texture: true })
  }
}

// ── shape drawing ─────────────────────────────────────────────────────────────────────────────
function drawBody(root: Container, kind: number, team: number): void {
  const g = new Graphics()
  const col = team === NEUTRAL ? 0x52e0c4 : TEAM_COLOR[team as Team]
  const dark = shade(col, 0.55)
  const light = shade(col, 1.35)
  const r = STATS[kind as Kind]?.radius ?? 12
  switch (kind) {
    case Kind.HQ: {
      g.roundRect(-r, -r, r * 2, r * 2, 10)
        .fill(dark)
        .stroke({ color: light, width: 3 })
      g.roundRect(-r * 0.5, -r * 0.5, r, r, 6).fill(col)
      g.circle(0, 0, r * 0.26).fill(light)
      break
    }
    case Kind.BARRACKS: {
      g.roundRect(-r, -r * 0.8, r * 2, r * 1.6, 6)
        .fill(dark)
        .stroke({ color: light, width: 3 })
      g.rect(-r * 0.8, -r * 0.5, r * 1.6, r * 0.3).fill(col)
      g.rect(-r * 0.8, 0, r * 1.6, r * 0.3).fill(col)
      break
    }
    case Kind.FACTORY: {
      g.roundRect(-r, -r * 0.8, r * 2, r * 1.6, 6)
        .fill(dark)
        .stroke({ color: light, width: 3 })
      g.circle(-r * 0.35, 0, r * 0.4).fill(col)
      g.circle(r * 0.45, r * 0.1, r * 0.34).fill(col)
      break
    }
    case Kind.DEPOT: {
      hexagon(g, r).fill(dark).stroke({ color: light, width: 3 })
      hexagon(g, r * 0.5).fill(col)
      break
    }
    case Kind.WORKER: {
      g.circle(0, 0, r).fill(col).stroke({ color: dark, width: 2 })
      g.circle(r * 0.35, 0, r * 0.34).fill(light) // facing dot
      break
    }
    case Kind.SOLDIER: {
      g.poly([r * 1.2, 0, -r, -r * 0.9, -r * 0.4, 0, -r, r * 0.9])
        .fill(col)
        .stroke({ color: dark, width: 1.5 })
      break
    }
    case Kind.TANK: {
      g.roundRect(-r, -r * 0.75, r * 2, r * 1.5, 4)
        .fill(col)
        .stroke({ color: dark, width: 2 })
      g.rect(0, -r * 0.16, r * 1.7, r * 0.32).fill(dark) // barrel
      g.circle(0, 0, r * 0.5).fill(light)
      break
    }
    case Kind.CRYSTAL: {
      g.poly([0, -r, r * 0.8, 0, 0, r, -r * 0.8, 0])
        .fill({ color: 0x52e0c4, alpha: 0.95 })
        .stroke({ color: 0xbafff2, width: 2 })
      g.poly([0, -r * 0.5, r * 0.4, 0, 0, r * 0.5, -r * 0.4, 0]).fill(0xbafff2)
      break
    }
  }
  root.addChild(g)
}

function hexagon(g: Graphics, r: number): Graphics {
  const pts: number[] = []
  for (let i = 0; i < 6; i++) {
    const a = (Math.PI / 3) * i - Math.PI / 6
    pts.push(Math.cos(a) * r, Math.sin(a) * r)
  }
  return g.poly(pts)
}

function shade(color: number, factor: number): number {
  const r = Math.min(255, ((color >> 16) & 255) * factor)
  const gr = Math.min(255, ((color >> 8) & 255) * factor)
  const b = Math.min(255, (color & 255) * factor)
  return (r << 16) | (gr << 8) | b
}
