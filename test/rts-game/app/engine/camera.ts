export { Camera }

import type { Container } from 'pixi.js'
import { MAP_H, MAP_W } from '../../shared/constants'

const MIN_ZOOM = 0.22
const MAX_ZOOM = 1.15

/** Top-down camera: world coordinates → the pixi world container's transform. */
class Camera {
  x = MAP_W / 2
  y = MAP_H / 2
  zoom = 0.42
  viewW = 800
  viewH = 600

  resize(w: number, h: number): void {
    this.viewW = w
    this.viewH = h
    this.clamp()
  }

  worldToScreen(wx: number, wy: number): { x: number; y: number } {
    return { x: (wx - this.x) * this.zoom + this.viewW / 2, y: (wy - this.y) * this.zoom + this.viewH / 2 }
  }

  screenToWorld(sx: number, sy: number): { x: number; y: number } {
    return { x: (sx - this.viewW / 2) / this.zoom + this.x, y: (sy - this.viewH / 2) / this.zoom + this.y }
  }

  panByScreen(dx: number, dy: number): void {
    this.x += dx / this.zoom
    this.y += dy / this.zoom
    this.clamp()
  }

  zoomAt(sx: number, sy: number, factor: number): void {
    const before = this.screenToWorld(sx, sy)
    this.zoom = clamp(this.zoom * factor, MIN_ZOOM, MAX_ZOOM)
    this.x = before.x - (sx - this.viewW / 2) / this.zoom
    this.y = before.y - (sy - this.viewH / 2) / this.zoom
    this.clamp()
  }

  centerOn(wx: number, wy: number): void {
    this.x = wx
    this.y = wy
    this.clamp()
  }

  applyTo(container: Container): void {
    container.scale.set(this.zoom)
    container.position.set(this.viewW / 2 - this.x * this.zoom, this.viewH / 2 - this.y * this.zoom)
  }

  private clamp(): void {
    this.x = clamp(this.x, 0, MAP_W)
    this.y = clamp(this.y, 0, MAP_H)
  }
}

function clamp(v: number, lo: number, hi: number): number {
  return v < lo ? lo : v > hi ? hi : v
}
