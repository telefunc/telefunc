export { WsConnection }

import { TAG, encode, encodeCtrl, decode } from '../shared-ws.js'
import type { CtrlMessage } from '../shared-ws.js'
import { assert } from '../../utils/assert.js'
import { ReplayBuffer } from '../replay-buffer.js'

// ── Constants ──

const TTL_MS = 60_000
const PING_INTERVAL_MS = 5_000
const PONG_TIMEOUT_MS = 10_000
const RECONNECT_TIMEOUT_MS = 60_000
const RECONNECT_INITIAL_DELAY_MS = 500
const RECONNECT_MAX_DELAY_MS = 10_000
const CLIENT_REPLAY_BYTES = 1024 * 1024

/** Minimal interface that WsConnection needs from a channel.
 *  Implemented by ClientChannel. */
interface WsChannel {
  readonly id: string
  _onWsOpen(): void
  _onWsMessage(data: string): void
  _onWsBinaryMessage(data: Uint8Array): void
  _onWsClose(): void
}

/**
 * Multiplexed WebSocket connection shared by all client channels.
 *
 * A single WsConnection is created per server URL. Multiple channels
 * are multiplexed over it using client-assigned integer indices as
 * part of a compact binary framing protocol (see shared-ws.ts).
 *
 * Lifecycle:
 *  - First channel creates the connection and triggers a WebSocket open.
 *  - Additional channels call `register()` and send a CTRL `open` message.
 *  - When a channel closes, it calls `unregister()`.
 *  - When the last channel unregisters, an idle TTL starts. If no new
 *    channel registers before the TTL expires, the WebSocket closes cleanly.
 *
 * Reconnection:
 *  - On disconnect, the connection retries with exponential backoff.
 *  - On every (re)connect, a reconcile handshake runs:
 *      1. Client sends all its open channel ids + indices.
 *      2. Server replies with the subset it still has alive.
 *      3. Client closes any channels the server didn't acknowledge.
 *  - Buffered frames are held during reconnect and flushed after reconcile.
 *
 * Health:
 *  - Client sends CTRL pings every 5s; server replies with pong.
 *  - If no pong arrives within 10s, the connection is presumed dead
 *    and a reconnect is scheduled.
 */
class WsConnection {
  closed = false

  // ── Connection cache — one WsConnection per base URL ──
  private static cache = new Map<string, WsConnection>()

  /** Get or create a shared connection for the given telefunc URL. */
  static getOrCreate(telefuncUrl: string, channel: WsChannel): WsConnection {
    const wsBaseUrl = deriveWsUrl(telefuncUrl)
    let connection = WsConnection.cache.get(wsBaseUrl)
    if (!connection || connection.closed) {
      connection = new WsConnection(wsBaseUrl)
    }
    connection.register(channel)
    return connection
  }

  private cacheKey: string
  private ws: WebSocket | null = null
  private connected = false
  private nextIndex = 0
  private reconciling = false

  private channels = new Map<number, WsChannel>()
  private channelIndex = new Map<WsChannel, number>()
  private sendBuffer: Array<{ data: Uint8Array<ArrayBuffer>; channelIx: number | null }> = []
  /** Highest sequence number received from the server, per channel index. */
  private lastSeqByChannel = new Map<number, number>()
  /** Per-channel outgoing replay buffers (1 MB each). */
  private replayBuffers = new Map<number, ReplayBuffer>()

  // Timers
  private ttl: ReturnType<typeof setTimeout> | null = null
  private pingInterval: ReturnType<typeof setInterval> | null = null
  private pongTimer: ReturnType<typeof setTimeout> | null = null
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private reconnectAttempt = 0
  private reconnectStart = 0

  private constructor(cacheKey: string) {
    this.cacheKey = cacheKey
    WsConnection.cache.set(cacheKey, this)
  }

  /** Register a channel over this connection. */
  private register(channel: WsChannel): void {
    this.clearTimer('ttl')
    this.addChannel(channel)
    if (!this.ws && !this.connected) {
      // First channel — open the WebSocket.
      this.connect()
    } else if (this.connected && !this.reconciling) {
      // Already connected — reconcile so the server learns about the new channel.
      this.startReconcile()
    }
    // Otherwise connecting or reconciling — onopen/reconciled will pick it up.
  }

  /** Remove a channel (client-initiated close). */
  unregister(channel: WsChannel): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    this.channels.delete(ix)
    this.channelIndex.delete(channel)
    this.lastSeqByChannel.delete(ix)
    this.replayBuffers.delete(ix)
    this.trySend(encodeCtrl({ t: 'close', ix }), ix)
    this.startTtlIfIdle()
  }

  /** Send a text frame for the given channel. */
  send(channel: WsChannel, data: string): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const frame = encode.text(ix, data, seq)
    replay.push(seq, frame)
    this.trySend(frame, ix)
  }

  /** Send a binary frame for the given channel. */
  sendBinary(channel: WsChannel, data: Uint8Array): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const frame = encode.binary(ix, data, seq)
    replay.push(seq, frame)
    this.trySend(frame, ix)
  }

  /** Send a CTRL pause for the given channel. */
  sendPause(channel: WsChannel): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    this.trySend(encodeCtrl({ t: 'pause', ix }), ix)
  }

  /** Send a CTRL resume for the given channel. */
  sendResume(channel: WsChannel): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    this.trySend(encodeCtrl({ t: 'resume', ix }), ix)
  }

  // ── Internals ──

  private addChannel(channel: WsChannel): number {
    const ix = this.nextIndex++
    this.channels.set(ix, channel)
    this.channelIndex.set(channel, ix)
    this.replayBuffers.set(ix, new ReplayBuffer(CLIENT_REPLAY_BYTES))
    return ix
  }

  private connect(): void {
    if (this.closed) return

    // Use the first channel's id in the URL so the server can validate the upgrade.
    const firstChannel = this.channels.values().next().value
    const channelId = firstChannel ? firstChannel.id : ''
    const wsUrl = this.cacheKey + '?channelId=' + encodeURIComponent(channelId)

    try {
      this.ws = new WebSocket(wsUrl)
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
          if (frame.seq && !this.trackSeq(frame.index, frame.seq)) break
          this.channels.get(frame.index)?._onWsMessage(frame.text)
          break
        case TAG.BINARY:
          if (frame.seq && !this.trackSeq(frame.index, frame.seq)) break
          this.channels.get(frame.index)?._onWsBinaryMessage(frame.data)
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

  private startReconcile(): void {
    this.reconciling = true
    const open: { id: string; ix: number; lastSeq: number }[] = []
    for (const [ix, ch] of this.channels) {
      open.push({ id: ch.id, ix, lastSeq: this.lastSeqByChannel.get(ix) ?? 0 })
    }
    const ws = this.ws
    assert(ws)
    ws.send(encodeCtrl({ t: 'reconcile', open }))
  }

  /** Track seq for a channel. Returns false if this is a duplicate (already seen). */
  private trackSeq(ix: number, seq: number): boolean {
    const prev = this.lastSeqByChannel.get(ix) ?? 0
    if (seq <= prev) return false
    this.lastSeqByChannel.set(ix, seq)
    return true
  }

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

  private handleCtrl(ctrl: CtrlMessage): void {
    if (!ctrl || typeof ctrl !== 'object') return
    switch (ctrl.t) {
      case 'pong':
        this.resetPongTimer()
        break

      case 'close': {
        const ch = this.channels.get(ctrl.ix)
        if (!ch) break
        this.channels.delete(ctrl.ix)
        this.channelIndex.delete(ch)
        this.lastSeqByChannel.delete(ctrl.ix)
        this.replayBuffers.delete(ctrl.ix)
        ch._onWsClose()
        this.startTtlIfIdle()
        break
      }

      case 'reconciled': {
        const serverMap = new Map(ctrl.open.map((c) => [c.ix, c.lastSeq]))
        for (const [ix, ch] of this.channels) {
          if (!serverMap.has(ix)) {
            this.channels.delete(ix)
            this.channelIndex.delete(ch)
            this.lastSeqByChannel.delete(ix)
            this.replayBuffers.delete(ix)
            ch._onWsClose()
          } else {
            // Replay missed client→server frames for this channel
            const lastSeq = serverMap.get(ix)!
            const replay = this.replayBuffers.get(ix)
            if (replay) {
              const missed = replay.getAfter(lastSeq)
              const ws = this.ws
              if (ws) for (const frame of missed) ws.send(frame as Uint8Array<ArrayBuffer>)
            }
            ch._onWsOpen()
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
    for (const ch of this.channels.values()) ch._onWsClose()
    this.channels.clear()
    this.channelIndex.clear()
    this.lastSeqByChannel.clear()
    this.replayBuffers.clear()
    WsConnection.cache.delete(this.cacheKey)
  }

  private dispose(): void {
    this.closed = true
    WsConnection.cache.delete(this.cacheKey)
  }

  private clearTimer(name: 'ttl' | 'pongTimer' | 'pingInterval' | 'reconnectTimer'): void {
    const timer = this[name]
    if (timer) {
      clearTimeout(timer as ReturnType<typeof setTimeout>)
      ;(this as any)[name] = null
    }
  }
}

/** Convert a telefunc HTTP URL to a WebSocket base URL. */
function deriveWsUrl(telefuncUrl: string): string {
  const base = telefuncUrl.startsWith('http') ? telefuncUrl : location.origin + telefuncUrl
  const url = new URL(base)
  url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'
  return url.toString()
}
