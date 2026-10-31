export { InputController }
export type { SelectionInfo }

import { isBuildingKind, Kind, STATS, type Team } from '../../shared/constants'
import type { Command } from '../../shared/types'
import type { Camera } from './camera'
import { WorldBuffer } from './world-buffer'

type SelectionInfo = {
  ids: number[]
  kinds: number[] // kinds present in the selection
  building: { id: number; kind: number } | null // set when a single building is selected
  hasWorker: boolean
}

type Deps = {
  canvas: HTMLCanvasElement
  camera: Camera
  buffer: WorldBuffer
  getMyTeam: () => Team
  send: (cmd: Command) => void
  onSelect: (info: SelectionInfo) => void
  addMarker: (x: number, y: number, kind: 'move' | 'attack' | 'rally') => void
}

/** All in-match pointer/keyboard interaction: selection, contextual orders, build placement, and
 *  camera intent (arrow keys + edge scroll, read by the game loop). Production/build *initiation*
 *  comes from the HUD (React), which calls `beginPlacement` / issues produce commands directly. */
class InputController {
  selection = new Set<number>()
  box: { x0: number; y0: number; x1: number; y1: number } | null = null
  placement: { kind: number } | null = null
  mouse = { x: 0, y: 0, over: false }
  panKeys = { up: false, down: false, left: false, right: false }
  private attackMove = false
  private downPt: { x: number; y: number } | null = null
  private d: Deps

  constructor(deps: Deps) {
    this.d = deps
    const c = deps.canvas
    c.addEventListener('pointerdown', this.onDown)
    c.addEventListener('pointermove', this.onMove)
    window.addEventListener('pointerup', this.onUp)
    c.addEventListener('contextmenu', this.onContext)
    c.addEventListener('wheel', this.onWheel, { passive: false })
    c.addEventListener('pointerleave', this.onLeave)
    window.addEventListener('keydown', this.onKey)
    window.addEventListener('keyup', this.onKeyUp)
  }

  beginPlacement(kind: number): void {
    // Only meaningful with a worker selected.
    if (this.selectionInfo().hasWorker) this.placement = { kind }
  }

  cancelModes(): void {
    this.placement = null
    this.attackMove = false
  }

  /** The build placement ghost for the renderer (with validity). */
  placementGhost(): { kind: number; x: number; y: number; valid: boolean } | null {
    if (!this.placement || !this.mouse.over) return null
    const w = this.d.camera.screenToWorld(this.mouse.x, this.mouse.y)
    return { kind: this.placement.kind, x: w.x, y: w.y, valid: this.placementValid(w.x, w.y, this.placement.kind) }
  }

  private onDown = (e: PointerEvent): void => {
    if (e.button !== 0) return
    const rect = this.d.canvas.getBoundingClientRect()
    const sx = e.clientX - rect.left
    const sy = e.clientY - rect.top
    const w = this.d.camera.screenToWorld(sx, sy)

    if (this.placement) {
      this.tryPlace(w.x, w.y)
      return
    }
    if (this.attackMove && this.selection.size > 0) {
      this.d.send({ t: 'attackMove', ids: [...this.selection], x: w.x, y: w.y })
      this.d.addMarker(w.x, w.y, 'attack')
      this.attackMove = false
      return
    }
    this.downPt = { x: sx, y: sy }
    this.box = { x0: sx, y0: sy, x1: sx, y1: sy }
  }

  private onMove = (e: PointerEvent): void => {
    const rect = this.d.canvas.getBoundingClientRect()
    this.mouse.x = e.clientX - rect.left
    this.mouse.y = e.clientY - rect.top
    this.mouse.over = true
    if (this.box) {
      this.box.x1 = this.mouse.x
      this.box.y1 = this.mouse.y
    }
  }

  private onUp = (e: PointerEvent): void => {
    if (e.button !== 0 || !this.box || !this.downPt) return
    const dragged = Math.hypot(this.box.x1 - this.downPt.x, this.box.y1 - this.downPt.y)
    if (dragged < 6) this.clickSelect(this.downPt.x, this.downPt.y)
    else this.boxSelect(this.box)
    this.box = null
    this.downPt = null
  }

  private onContext = (e: MouseEvent): void => {
    e.preventDefault()
    if (this.placement || this.attackMove) {
      this.cancelModes()
      return
    }
    const rect = this.d.canvas.getBoundingClientRect()
    const w = this.d.camera.screenToWorld(e.clientX - rect.left, e.clientY - rect.top)
    this.issueOrder(w.x, w.y)
  }

  private onWheel = (e: WheelEvent): void => {
    e.preventDefault()
    const rect = this.d.canvas.getBoundingClientRect()
    this.d.camera.zoomAt(e.clientX - rect.left, e.clientY - rect.top, e.deltaY < 0 ? 1.12 : 0.89)
  }

  private onLeave = (): void => {
    this.mouse.over = false
    this.panKeys.up = this.panKeys.down = this.panKeys.left = this.panKeys.right = false
  }

  private onKey = (e: KeyboardEvent): void => {
    if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return
    switch (e.key) {
      case 'Escape':
        this.cancelModes()
        this.setSelection([])
        break
      case 'a':
      case 'A':
        if (this.selection.size > 0) this.attackMove = true
        break
      case 's':
      case 'S':
        if (this.selection.size > 0) this.d.send({ t: 'stop', ids: [...this.selection] })
        break
      case 'ArrowUp':
        this.panKeys.up = true
        break
      case 'ArrowDown':
        this.panKeys.down = true
        break
      case 'ArrowLeft':
        this.panKeys.left = true
        break
      case 'ArrowRight':
        this.panKeys.right = true
        break
    }
  }

  private onKeyUp = (e: KeyboardEvent): void => {
    if (e.key === 'ArrowUp') this.panKeys.up = false
    if (e.key === 'ArrowDown') this.panKeys.down = false
    if (e.key === 'ArrowLeft') this.panKeys.left = false
    if (e.key === 'ArrowRight') this.panKeys.right = false
  }

  // ── selection ──────────────────────────────────────────────────────────────────────────────
  private clickSelect(sx: number, sy: number): void {
    const hit = this.pick(sx, sy)
    if (!hit) {
      this.setSelection([])
      return
    }
    const mine = hit.team === this.d.getMyTeam()
    if (mine) this.setSelection([hit.id])
    else this.setSelection([]) // clicking an enemy just deselects; right-click attacks
  }

  private boxSelect(box: { x0: number; y0: number; x1: number; y1: number }): void {
    const a = this.d.camera.screenToWorld(box.x0, box.y0)
    const b = this.d.camera.screenToWorld(box.x1, box.y1)
    const minX = Math.min(a.x, b.x)
    const maxX = Math.max(a.x, b.x)
    const minY = Math.min(a.y, b.y)
    const maxY = Math.max(a.y, b.y)
    const ids: number[] = []
    const team = this.d.getMyTeam()
    for (const e of this.d.buffer.entities.values()) {
      if (e.team !== team || isBuildingKind(e.kind) || e.kind === Kind.CRYSTAL) continue
      const s = WorldBuffer.sample(e, performance.now())
      if (s.x >= minX && s.x <= maxX && s.y >= minY && s.y <= maxY) ids.push(e.id)
    }
    // Box that selected no units keeps the current selection (feels better than clearing).
    if (ids.length > 0) this.setSelection(ids)
  }

  private pick(sx: number, sy: number): { id: number; team: number; kind: number } | null {
    const now = performance.now()
    let best: { id: number; team: number; kind: number } | null = null
    let bestD = Infinity
    for (const e of this.d.buffer.entities.values()) {
      const s = WorldBuffer.sample(e, now)
      const scr = this.d.camera.worldToScreen(s.x, s.y)
      const rPx = (STATS[e.kind as Kind]?.radius ?? 12) * this.d.camera.zoom + 6
      const d = Math.hypot(scr.x - sx, scr.y - sy)
      if (d <= rPx && d < bestD) {
        bestD = d
        best = { id: e.id, team: e.team, kind: e.kind }
      }
    }
    return best
  }

  private issueOrder(wx: number, wy: number): void {
    const info = this.selectionInfo()
    if (info.building && info.ids.length === 1) {
      this.d.send({ t: 'rally', building: info.building.id, x: wx, y: wy })
      this.d.addMarker(wx, wy, 'rally')
      return
    }
    if (info.ids.length === 0) return
    const scr = this.d.camera.worldToScreen(wx, wy)
    const target = this.pick(scr.x, scr.y)
    const team = this.d.getMyTeam()
    if (target && target.team !== team && target.kind !== Kind.CRYSTAL && target.team !== 0) {
      this.d.send({ t: 'attack', ids: info.ids, target: target.id })
      this.d.addMarker(wx, wy, 'attack')
    } else if (target && target.kind === Kind.CRYSTAL && info.hasWorker) {
      this.d.send({ t: 'gather', ids: info.ids, target: target.id })
      this.d.addMarker(wx, wy, 'move')
    } else {
      this.d.send({ t: 'move', ids: info.ids, x: wx, y: wy })
      this.d.addMarker(wx, wy, 'move')
    }
  }

  private tryPlace(wx: number, wy: number): void {
    const p = this.placement
    if (!p) return
    const worker = this.firstSelectedWorker()
    if (worker !== null && this.placementValid(wx, wy, p.kind)) {
      this.d.send({ t: 'build', worker, building: p.kind, x: wx, y: wy })
      this.d.addMarker(wx, wy, 'rally')
      this.placement = null
    }
  }

  private placementValid(wx: number, wy: number, kind: number): boolean {
    const r = STATS[kind as Kind]?.radius ?? 40
    if (wx < r || wy < r) return false
    for (const e of this.d.buffer.entities.values()) {
      if (!isBuildingKind(e.kind) && e.kind !== Kind.CRYSTAL) continue
      const min = (STATS[e.kind as Kind]?.radius ?? 40) + r + 12
      const s = WorldBuffer.sample(e, performance.now())
      if (Math.hypot(s.x - wx, s.y - wy) < min) return false
    }
    return true
  }

  private firstSelectedWorker(): number | null {
    for (const id of this.selection) {
      const e = this.d.buffer.entities.get(id)
      if (e && e.kind === Kind.WORKER) return id
    }
    return null
  }

  private setSelection(ids: number[]): void {
    this.selection = new Set(ids)
    this.d.onSelect(this.selectionInfo())
  }

  /** Recompute the selection summary against the *current* world (entities may have died). */
  selectionInfo(): SelectionInfo {
    const kinds = new Set<number>()
    let hasWorker = false
    let building: { id: number; kind: number } | null = null
    const ids: number[] = []
    for (const id of this.selection) {
      const e = this.d.buffer.entities.get(id)
      if (!e) continue
      ids.push(id)
      kinds.add(e.kind)
      if (e.kind === Kind.WORKER) hasWorker = true
      if (isBuildingKind(e.kind) && this.selection.size === 1) building = { id: e.id, kind: e.kind }
    }
    return { ids, kinds: [...kinds], building, hasWorker }
  }

  dispose(): void {
    const c = this.d.canvas
    c.removeEventListener('pointerdown', this.onDown)
    c.removeEventListener('pointermove', this.onMove)
    window.removeEventListener('pointerup', this.onUp)
    c.removeEventListener('contextmenu', this.onContext)
    c.removeEventListener('wheel', this.onWheel)
    c.removeEventListener('pointerleave', this.onLeave)
    window.removeEventListener('keydown', this.onKey)
    window.removeEventListener('keyup', this.onKeyUp)
  }
}
