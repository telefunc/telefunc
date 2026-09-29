export { Game }
export type { HudSnapshot }

import { isBuildingKind, Kind, NEUTRAL, type Team } from '../../shared/constants'
import type { Command } from '../../shared/types'
import { sendCommand, worldBuffer } from '../net'
import { Camera } from './camera'
import { Fog } from './fog'
import { InputController, type SelectionInfo } from './input'
import { Minimap } from './minimap'
import { SceneRenderer } from './scene'

type HudSnapshot = {
  crystal: number
  supplyUsed: number
  supplyCap: number
  clockSec: number
  winner: Team
  myUnits: number
  myBuildings: number
  selection: SelectionInfo
}

const EDGE = 26
const PAN_SPEED = 900

/** The match client: owns the pixi scene, camera, input, minimap and the render loop. Reads the
 *  fog-filtered world from `net`'s `worldBuffer`; sends orders back through `net`. Created when the
 *  match view mounts, disposed when it unmounts. */
class Game {
  input!: InputController
  private scene = new SceneRenderer()
  private camera = new Camera()
  private fog = new Fog()
  private minimap: Minimap | null = null
  private raf = 0
  private last = 0
  private centered = false
  private disposed = false
  private hudTimer: ReturnType<typeof setInterval> | null = null
  private markers: { x: number; y: number; at: number; kind: 'move' | 'attack' | 'rally' }[] = []

  constructor(
    private host: HTMLElement,
    private getMyTeam: () => Team,
    private onSelect: (info: SelectionInfo) => void,
    private onHud: (hud: HudSnapshot) => void,
  ) {}

  async start(): Promise<void> {
    // The HUD pump is decoupled from the WebGL render loop: economy, roster and result come from
    // the decoded stream (net's worldBuffer), not the renderer, so they reach the UI even while a
    // GPU-less runner is still spinning up its software WebGL context. Started before `scene.init`
    // for exactly that reason.
    this.hudTimer = setInterval(() => this.pushHud(), 150)
    await this.scene.init(this.host)
    if (this.disposed) return
    this.scene.attachFog(this.fog)
    this.fog.setEnabled(this.getMyTeam() !== NEUTRAL)
    this.camera.resize(this.host.clientWidth, this.host.clientHeight)
    this.input = new InputController({
      canvas: this.scene.app.canvas as HTMLCanvasElement,
      camera: this.camera,
      buffer: worldBuffer,
      getMyTeam: this.getMyTeam,
      send: (cmd: Command) => sendCommand(cmd),
      onSelect: this.onSelect,
      addMarker: (x, y, kind) => this.markers.push({ x, y, at: performance.now(), kind }),
    })
    this.last = performance.now()
    this.loop()
  }

  attachMinimap(canvas: HTMLCanvasElement): void {
    this.minimap = new Minimap(canvas, this.camera, worldBuffer, this.getMyTeam)
  }

  /** HUD → engine: begin placing a building with the selected worker. */
  beginPlacement(kind: number): void {
    this.input.beginPlacement(kind)
  }

  private loop = (): void => {
    if (this.disposed) return
    const now = performance.now()
    const dt = Math.min(0.05, (now - this.last) / 1000)
    this.last = now

    this.camera.resize(this.host.clientWidth, this.host.clientHeight)
    this.applyPan(dt)
    this.centerOnBaseOnce()

    this.markers = this.markers.filter((m) => now - m.at < 560)
    this.scene.frame({
      now,
      camera: this.camera,
      buffer: worldBuffer,
      fog: this.fog,
      myTeam: this.getMyTeam(),
      selection: this.input.selection,
      box: this.input.box,
      markers: this.markers,
      placement: this.input.placementGhost(),
    })
    this.minimap?.draw()
    this.raf = requestAnimationFrame(this.loop)
  }

  private applyPan(dt: number): void {
    const k = this.input.panKeys
    let dx = 0
    let dy = 0
    if (k.left) dx -= 1
    if (k.right) dx += 1
    if (k.up) dy -= 1
    if (k.down) dy += 1
    // Edge scroll.
    const m = this.input.mouse
    if (m.over) {
      if (m.x < EDGE) dx -= 1
      else if (m.x > this.camera.viewW - EDGE) dx += 1
      if (m.y < EDGE) dy -= 1
      else if (m.y > this.camera.viewH - EDGE) dy += 1
    }
    if (dx || dy) this.camera.panByScreen(dx * PAN_SPEED * dt, dy * PAN_SPEED * dt)
  }

  private centerOnBaseOnce(): void {
    if (this.centered) return
    const team = this.getMyTeam()
    for (const e of worldBuffer.entities.values()) {
      if (e.kind === Kind.HQ && (e.team === team || team === NEUTRAL)) {
        this.camera.centerOn(e.toX, e.toY)
        this.centered = true
        return
      }
    }
  }

  private pushHud(): void {
    const team = this.getMyTeam()
    let myUnits = 0
    let myBuildings = 0
    for (const e of worldBuffer.entities.values()) {
      if (e.team !== team) continue
      if (isBuildingKind(e.kind)) myBuildings++
      else if (e.kind !== Kind.CRYSTAL) myUnits++
    }
    this.onHud({
      crystal: worldBuffer.crystal,
      supplyUsed: worldBuffer.supplyUsed,
      supplyCap: worldBuffer.supplyCap,
      clockSec: worldBuffer.clockSec,
      winner: worldBuffer.winner as Team,
      myUnits,
      myBuildings,
      selection: this.input?.selectionInfo() ?? { ids: [], kinds: [], building: null, hasWorker: false },
    })
  }

  dispose(): void {
    if (this.disposed) return
    this.disposed = true
    if (this.hudTimer) clearInterval(this.hudTimer)
    cancelAnimationFrame(this.raf)
    this.input?.dispose()
    this.minimap?.dispose()
    this.scene.dispose()
  }
}
