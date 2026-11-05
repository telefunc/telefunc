export { Minimap }

import { isBuildingKind, Kind, MAP_H, MAP_W, NEUTRAL, STATS, TEAM_COLOR, type Team } from '../../shared/constants'
import type { Camera } from './camera'
import { WorldBuffer } from './world-buffer'

/** A plain 2D-canvas minimap. It draws only what's in the world buffer — which is already fog-
 *  filtered by the server — so it naturally shows just your team's vision, no extra masking. */
class Minimap {
  private ctx: CanvasRenderingContext2D
  private size: number
  private dragging = false

  constructor(
    private canvas: HTMLCanvasElement,
    private camera: Camera,
    private buffer: WorldBuffer,
    private getMyTeam: () => Team,
  ) {
    this.size = canvas.width
    this.ctx = canvas.getContext('2d')!
    canvas.addEventListener('pointerdown', this.onDown)
    canvas.addEventListener('pointermove', this.onMove)
    window.addEventListener('pointerup', this.onUp)
  }

  private onDown = (e: PointerEvent): void => {
    this.dragging = true
    this.jump(e)
  }
  private onMove = (e: PointerEvent): void => {
    if (this.dragging) this.jump(e)
  }
  private onUp = (): void => {
    this.dragging = false
  }

  private jump(e: PointerEvent): void {
    const rect = this.canvas.getBoundingClientRect()
    const mx = ((e.clientX - rect.left) / rect.width) * this.size
    const my = ((e.clientY - rect.top) / rect.height) * this.size
    this.camera.centerOn((mx / this.size) * MAP_W, (my / this.size) * MAP_H)
  }

  draw(): void {
    const ctx = this.ctx
    const s = this.size
    ctx.fillStyle = '#0a0e17'
    ctx.fillRect(0, 0, s, s)
    ctx.strokeStyle = '#1e2740'
    ctx.strokeRect(0.5, 0.5, s - 1, s - 1)

    const sx = s / MAP_W
    const sy = s / MAP_H
    const now = performance.now()
    for (const e of this.buffer.entities.values()) {
      const p = WorldBuffer.sample(e, now)
      const col = e.team === NEUTRAL ? '#52e0c4' : e.team === this.getMyTeam() ? teamHex(e.team) : '#ff5a5a'
      const r = isBuildingKind(e.kind) ? 2.6 : e.kind === Kind.CRYSTAL ? 1.6 : 1.4
      ctx.fillStyle = col
      ctx.beginPath()
      ctx.arc(p.x * sx, p.y * sy, r, 0, Math.PI * 2)
      ctx.fill()
    }

    // Viewport rectangle.
    const tl = this.camera.screenToWorld(0, 0)
    const br = this.camera.screenToWorld(this.camera.viewW, this.camera.viewH)
    ctx.strokeStyle = 'rgba(255,255,255,0.7)'
    ctx.lineWidth = 1
    ctx.strokeRect(tl.x * sx, tl.y * sy, (br.x - tl.x) * sx, (br.y - tl.y) * sy)
  }

  dispose(): void {
    this.canvas.removeEventListener('pointerdown', this.onDown)
    this.canvas.removeEventListener('pointermove', this.onMove)
    window.removeEventListener('pointerup', this.onUp)
  }
}

function teamHex(team: Team): string {
  return '#' + TEAM_COLOR[team].toString(16).padStart(6, '0')
}
