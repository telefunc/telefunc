export { WsConnection }
export type { ChannelCallbacks, ChannelHandle }

import { TAG, encode, encodeCtrl, decode } from '../shared-ws.js'
import type { CtrlMessage } from '../shared-ws.js'
import { assert } from '../../utils/assert.js'

// ── Constants ──

const TTL_MS = 60_000
const PING_INTERVAL_MS = 5_000
const PONG_TIMEOUT_MS = 10_000
const RECONNECT_TIMEOUT_MS = 60_000
const RECONNECT_INITIAL_DELAY_MS = 500
const RECONNECT_MAX_DELAY_MS = 10_000

// ── Types ──

/** Callbacks from the WS layer into the client channel. */
type ChannelCallbacks = {
  onOpen(): void
  onMessage(data: string): void
  onBinaryMessage(data: Uint8Array): void
  onClose(): void
}

/** Per-channel handle for sending data and closing. */
type ChannelHandle = {
  send(data: string): void
  sendBinary(data: Uint8Array): void
  pause(): void
  resume(): void
  close(): void
}

/** Entry in the send buffer — channelIx allows dead-channel filtering on flush. */
type BufferedFrame = {
  data: Uint8Array<ArrayBuffer>
  channelIx: number | null
}

type Channel = {
  index: number
  channelId: string
  callbacks: ChannelCallbacks
}

/**
 * Multiplexed WebSocket connection.
 *
 * Channel indices are client-owned and stable for the channel's lifetime.
 * sendBuffer is never cleared — frames keep their original indices.
 *
 * Reconcile is symmetric — on every connect (first or reconnect):
 *   1. Client sends `reconcile { open }` — all its open channels.
 *   2. Server attaches alive channels, ignores dead ones.
 *   3. Server responds `reconciled { open }` — all channels it attached.
 *   4. Client diffs: channels it has but server didn't mention → onClose().
 *
 * The reconciled response is the SOLE close-notification mechanism during reconcile.
 */
class WsConnection {
  readonly primaryHandle: ChannelHandle
  closed = false

  private wsUrl: string
  private ws: WebSocket | null = null
  private connected = false
  private onDispose: () => void
  private nextIndex = 0
  /** True while waiting for CTRL reconciled. sendBuffer is flushed after. */
  private reconciling = false

  private channels = new Map<number, Channel>()
  private sendBuffer: BufferedFrame[] = []

  // Timers
  private ttl: ReturnType<typeof setTimeout> | null = null
  private pingInterval: ReturnType<typeof setInterval> | null = null
  private pongTimer: ReturnType<typeof setTimeout> | null = null
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private reconnectAttempt = 0
  private reconnectStart = 0

  constructor(wsUrl: string, channelId: string, callbacks: ChannelCallbacks, onDispose: () => void) {
    this.wsUrl = wsUrl
    this.onDispose = onDispose
    const ix = this.nextIndex++
    const ch: Channel = { index: ix, channelId, callbacks }
    this.channels.set(ix, ch)
    this.primaryHandle = this.makeHandle(ch)
    this.connect()
  }

  /** Register an additional channel over this connection. */
  register(channelId: string, callbacks: ChannelCallbacks): ChannelHandle {
    this.clearTimer('ttl')
    const ix = this.nextIndex++
    const ch: Channel = { index: ix, channelId, callbacks }
    this.channels.set(ix, ch)
    this.trySend(encodeCtrl({ t: 'open', id: channelId, ix }), ix)
    return this.makeHandle(ch)
  }

  // ── Channel handle ──

  private makeHandle(ch: Channel): ChannelHandle {
    const ix = ch.index
    return {
      send: (data: string) => this.trySend(encode.text(ix, data), ix),
      sendBinary: (data: Uint8Array) => this.trySend(encode.binary(ix, data), ix),
      pause: () => this.trySend(encodeCtrl({ t: 'pause', ix }), ix),
      resume: () => this.trySend(encodeCtrl({ t: 'resume', ix }), ix),
      close: () => {
        this.channels.delete(ix)
        this.trySend(encodeCtrl({ t: 'close', ix }), ix)
        this.startTtlIfIdle()
      },
    }
  }

  // ── Connection ──

  private connect(): void {
    if (this.closed) return

    try {
      this.ws = new WebSocket(this.wsUrl)
    } catch {
      this.scheduleReconnect()
      return
    }

    this.ws.binaryType = 'arraybuffer'

    this.ws.onopen = () => {
      this.connected = true
      this.reconnectAttempt = 0
      this.reconnectStart = 0
      this.startReconcile()
      this.startPing()
    }

    this.ws.onmessage = ({ data }: MessageEvent) => {
      const frame = decode(new Uint8Array(data as ArrayBuffer))
      switch (frame.tag) {
        case TAG.CTRL:
          this.handleCtrl(frame.ctrl)
          break
        case TAG.TEXT:
          this.channels.get(frame.index)?.callbacks.onMessage(frame.text)
          break
        case TAG.BINARY:
          this.channels.get(frame.index)?.callbacks.onBinaryMessage(frame.data)
          break
      }
    }

    this.ws.onclose = () => {
      this.connected = false
      this.reconciling = false
      this.ws = null
      this.stopPing()
      this.clearTimer('ttl')

      if (this.closed) {
        this.notifyAllClosed()
      } else if (this.channels.size === 0) {
        this.dispose()
      } else {
        this.scheduleReconnect()
      }
    }

    this.ws.onerror = () => {} // always followed by onclose
  }

  // ── Reconcile ──

  /**
   * Send CTRL reconcile — "here's every channel I have open."
   * Server diffs with its state, replies reconciled.
   * sendBuffer is held until reconciled arrives (server needs the full picture first).
   */
  private startReconcile(): void {
    this.reconciling = true
    const open: { id: string; ix: number }[] = []
    for (const ch of this.channels.values()) open.push({ id: ch.channelId, ix: ch.index })
    const ws = this.ws
    assert(ws)
    ws.send(encodeCtrl({ t: 'reconcile', open }))
  }

  // ── Frame sending ──

  private trySend(frame: Uint8Array<ArrayBuffer>, channelIx?: number): void {
    if (this.ws && this.connected && !this.reconciling) {
      this.ws.send(frame)
    } else {
      this.sendBuffer.push({ data: frame, channelIx: channelIx ?? null })
    }
  }

  private flushSendBuffer(): void {
    const ws = this.ws
    assert(ws)
    const buf = this.sendBuffer
    this.sendBuffer = []
    for (const { data, channelIx } of buf) {
      if (channelIx !== null && !this.channels.has(channelIx)) continue
      ws.send(data)
    }
  }

  // ── CTRL handling ──

  private handleCtrl(ctrl: CtrlMessage): void {
    if (!ctrl || typeof ctrl !== 'object') return
    switch (ctrl.t) {
      case 'pong':
        this.resetPongTimer()
        break

      case 'opened': {
        const ch = this.channels.get(ctrl.ix)
        if (ch) ch.callbacks.onOpen()
        break
      }

      case 'close': {
        const ch = this.channels.get(ctrl.ix)
        if (!ch) break
        this.channels.delete(ctrl.ix)
        ch.callbacks.onClose()
        this.startTtlIfIdle()
        break
      }

      case 'reconciled': {
        // Server's truth: these are the channels it has alive.
        // Anything we have that server didn't mention → dead.
        const serverIxs = new Set(ctrl.open.map((c) => c.ix))
        for (const [ix, ch] of this.channels) {
          if (!serverIxs.has(ix)) {
            this.channels.delete(ix)
            ch.callbacks.onClose()
          } else {
            ch.callbacks.onOpen()
          }
        }
        this.reconciling = false
        this.flushSendBuffer()
        this.startTtlIfIdle()
        break
      }
    }
  }

  // ── Ping / pong ──

  private startPing(): void {
    this.resetPongTimer()
    this.pingInterval = setInterval(() => {
      if (this.ws && this.connected) this.ws.send(encodeCtrl({ t: 'ping' }))
    }, PING_INTERVAL_MS)
  }

  private stopPing(): void {
    this.clearTimer('pingInterval')
    this.clearTimer('pongTimer')
  }

  private resetPongTimer(): void {
    this.clearTimer('pongTimer')
    this.pongTimer = setTimeout(() => {
      this.stopPing()
      this.connected = false
      this.abandonWs()
      this.scheduleReconnect()
    }, PONG_TIMEOUT_MS)
  }

  /** Detach handlers and best-effort close (connection presumed dead). */
  private abandonWs(): void {
    const ws = this.ws
    if (!ws) return
    this.ws = null
    ws.onopen = ws.onclose = ws.onmessage = ws.onerror = null
    try {
      ws.close()
    } catch {
      /* connection may already be dead */
    }
  }

  // ── Reconnection ──

  private scheduleReconnect(): void {
    if (this.closed) return
    if (!this.reconnectStart) this.reconnectStart = Date.now()
    if (Date.now() - this.reconnectStart > RECONNECT_TIMEOUT_MS) {
      this.notifyAllClosed()
      return
    }
    const delay = Math.min(RECONNECT_INITIAL_DELAY_MS * 1.5 ** this.reconnectAttempt, RECONNECT_MAX_DELAY_MS)
    this.reconnectAttempt++
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null
      this.connect()
    }, delay)
  }

  // ── TTL ──

  private startTtlIfIdle(): void {
    if (this.channels.size > 0) return
    this.ttl = setTimeout(() => {
      if (this.channels.size === 0) this.closeGracefully()
    }, TTL_MS)
  }

  // ── Shutdown ──

  private closeGracefully(): void {
    this.stopPing()
    this.clearTimer('reconnectTimer')
    if (this.ws) {
      this.ws.close(1000)
      this.ws = null
    }
    this.dispose()
  }

  private notifyAllClosed(): void {
    this.closed = true
    for (const ch of this.channels.values()) ch.callbacks.onClose()
    this.channels.clear()
    this.onDispose()
  }

  private dispose(): void {
    this.closed = true
    this.onDispose()
  }

  private clearTimer(name: 'ttl' | 'pongTimer' | 'pingInterval' | 'reconnectTimer'): void {
    const timer = this[name]
    if (timer) {
      clearTimeout(timer as ReturnType<typeof setTimeout>)
      ;(this as any)[name] = null
    }
  }
}
