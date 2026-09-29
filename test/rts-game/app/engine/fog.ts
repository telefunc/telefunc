export { Fog }

import { Kind, MAP_H, MAP_W, NEUTRAL, STATS, type Team } from '../../shared/constants'
import type { RenderEntity } from './world-buffer'

const TILE = 80
const W = Math.ceil(MAP_W / TILE)
const H = Math.ceil(MAP_H / TILE)

// Client-side fog of war. The server already filters enemy entities by team vision, so the client
// only needs to *draw* the fog — and it can, entirely from its own units, because both sides
// compute sight from the same STATS table (see shared/constants.ts). This is what lets the wire
// carry no fog mask: the client reconstructs the exact revealed region the server chose to reveal.
class Fog {
  readonly canvas: HTMLCanvasElement
  private ctx: CanvasRenderingContext2D
  private image: ImageData
  private visible = new Uint8Array(W * H)
  private explored = new Uint8Array(W * H)
  private enabled = true

  constructor() {
    this.canvas = document.createElement('canvas')
    this.canvas.width = W
    this.canvas.height = H
    this.ctx = this.canvas.getContext('2d')!
    this.image = this.ctx.createImageData(W, H)
  }

  setEnabled(on: boolean): void {
    this.enabled = on
  }

  update(entities: Iterable<RenderEntity>, myTeam: Team, now: number): void {
    if (!this.enabled) return
    this.visible.fill(0)
    for (const e of entities) {
      if (e.team !== myTeam) continue
      const sight = STATS[e.kind as Kind]?.sight ?? 0
      if (sight <= 0) continue
      const s = e.segStart <= now ? sampleXY(e, now) : { x: e.toX, y: e.toY }
      this.light(s.x, s.y, sight)
    }
    for (let i = 0; i < this.visible.length; i++) if (this.visible[i]) this.explored[i] = 1
    this.paint()
  }

  isVisible(wx: number, wy: number): boolean {
    if (!this.enabled) return true
    const tx = clampi(Math.floor(wx / TILE), 0, W - 1)
    const ty = clampi(Math.floor(wy / TILE), 0, H - 1)
    return this.visible[ty * W + tx] === 1
  }

  private light(x: number, y: number, r: number): void {
    const minTx = Math.max(0, Math.floor((x - r) / TILE))
    const maxTx = Math.min(W - 1, Math.floor((x + r) / TILE))
    const minTy = Math.max(0, Math.floor((y - r) / TILE))
    const maxTy = Math.min(H - 1, Math.floor((y + r) / TILE))
    const r2 = r * r
    for (let ty = minTy; ty <= maxTy; ty++) {
      for (let tx = minTx; tx <= maxTx; tx++) {
        const cx = tx * TILE + TILE / 2
        const cy = ty * TILE + TILE / 2
        const dx = cx - x
        const dy = cy - y
        if (dx * dx + dy * dy <= r2) this.visible[ty * W + tx] = 1
      }
    }
  }

  private paint(): void {
    const data = this.image.data
    for (let i = 0; i < this.visible.length; i++) {
      const a = this.visible[i] ? 0 : this.explored[i] ? 120 : 236
      const p = i * 4
      data[p] = 3
      data[p + 1] = 6
      data[p + 2] = 14
      data[p + 3] = a
    }
    this.ctx.putImageData(this.image, 0, 0)
  }
}

function sampleXY(e: RenderEntity, now: number): { x: number; y: number } {
  const a = Math.min(1, (now - e.segStart) / 100)
  return { x: e.fromX + (e.toX - e.fromX) * a, y: e.fromY + (e.toY - e.fromY) * a }
}

function clampi(v: number, lo: number, hi: number): number {
  return v < lo ? lo : v > hi ? hi : v
}

// Spectators (no team) see everything.
export const spectatorTeam = NEUTRAL
