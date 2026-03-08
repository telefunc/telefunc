export { WsConnection }

import { TAG, encode, encodeCtrl, decode } from '../shared-ws.js'
import type { CtrlMessage } from '../shared-ws.js'
import { assert } from '../../utils/assert.js'
import { ReplayBuffer } from '../replay-buffer.js'
import { makeAbortError, makeBugError } from '../../client/remoteTelefunctionCall/errors.js'
import { ChannelClosedError, ChannelNetworkError } from '../channel-errors.js'
import { parse } from '@brillout/json-serializer/parse'
import {
  WS_RECONNECT_TIMEOUT,
  WS_IDLE_TIMEOUT,
  WS_PING_INTERVAL,
  WS_CLIENT_REPLAY_BUFFER,
  WS_RECONNECT_INITIAL_DELAY,
  WS_RECONNECT_MAX_DELAY,
} from '../constants.js'

// ── Constants ──

const PONG_TIMEOUT_MS = WS_PING_INTERVAL * 2

/** Minimal interface that WsConnection needs from a channel.
 *  Implemented by ClientChannel. */
interface WsChannel {
  readonly id: string
  readonly defer: boolean
  _onWsOpen(): void
  _onWsMessage(data: string): void
  _onWsBinaryMessage(data: Uint8Array): void
  _onWsAckReqMessage(data: string, seq: number): Promise<void>
  _onWsClose(err?: Error): void
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
      WsConnection.cache.set(wsBaseUrl, connection)
    }
    connection.register(channel)
    return connection
  }

  /** Per-channel pending ack promises. Key: `${channelIx}:${seq}`. */
  private pendingAcks = new Map<string, { resolve: (result: unknown) => void; reject: (err: Error) => void }>()

  private wsUrl: string
  private ws: WebSocket | null = null
  private connected = false
  private nextIndex = 0
  private reconciling = false
  /** Ixes sent in the current in-flight reconcile request. Cleared when reconciled arrives. */
  private reconcileIxes = new Set<number>()

  private channels = new Map<number, WsChannel>()
  private channelIndex = new Map<WsChannel, number>()
  private sendBuffer: Array<{ frame: Uint8Array<ArrayBuffer>; channelIx: number; seq?: number }> = []
  /** Highest sequence number received from the server, per channel index. */
  private lastSeqByChannel = new Map<number, number>()
  /** Per-channel outgoing replay buffers (1 MB each). */
  private replayBuffers = new Map<number, ReplayBuffer>()

  // Timers
  private ttl: ReturnType<typeof setTimeout> | null = null
  private pingInterval: ReturnType<typeof setInterval> | null = null
  private pongTimer: ReturnType<typeof setTimeout> | null = null
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private abandonedWs: WebSocket | null = null
  private reconnectAttempt = 0
  private reconnectStart = 0
  private reconnectTimeoutMs = WS_RECONNECT_TIMEOUT
  private idleTimeoutMs = WS_IDLE_TIMEOUT
  private pingIntervalMs = WS_PING_INTERVAL
  private pongTimeoutMs = PONG_TIMEOUT_MS
  private clientReplayBufferBytes = WS_CLIENT_REPLAY_BUFFER

  private constructor(wsUrl: string) {
    this.wsUrl = wsUrl
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

  /** Remove a channel from the connection's maps and start the idle TTL if no channels remain. */
  unregister(channel: WsChannel): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    this.channels.delete(ix)
    this.channelIndex.delete(channel)
    this.lastSeqByChannel.delete(ix)
    this.replayBuffers.delete(ix)
    this.clearPendingAcks(ix, new ChannelClosedError())
    this.startTtlIfIdle()
  }

  /** Send a text frame for the given channel. */
  send(channel: WsChannel, data: string): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const frame = encode.text(ix, data, seq)
    this.trySend(frame, ix, seq)
  }

  /** Send a TEXT_ACK_REQ frame. Returns a Promise resolved when the server sends ACK_RES. */
  sendTextAckReq(channel: WsChannel, data: string): Promise<unknown> {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return Promise.resolve(undefined)
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const frame = encode.textAckReq(ix, data, seq)
    this.trySend(frame, ix, seq)
    return new Promise<unknown>((resolve, reject) => this.pendingAcks.set(`${ix}:${seq}`, { resolve, reject }))
  }

  /** Send a binary frame for the given channel. */
  sendBinary(channel: WsChannel, data: Uint8Array): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const frame = encode.binary(ix, data, seq)
    this.trySend(frame, ix, seq)
  }

  /** Send an acknowledgement response for a message received from the server. */
  sendAckRes(channel: WsChannel, ackedSeq: number, result: string): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const replay = this.replayBuffers.get(ix)!
    const ownSeq = replay.nextSeq()
    const frame = encode.ackRes(ix, ownSeq, ackedSeq, result)
    this.trySend(frame, ix, ownSeq)
  }

  /** Send a CTRL close for the given channel and unregister it. */
  sendClose(channel: WsChannel): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    this.trySend(encodeCtrl({ t: 'close', ix }), ix) // buffer before unregister so ix is valid
    this.unregister(channel)
  }

  /** Send a CTRL abort for the given channel and unregister it. */
  sendAbort(channel: WsChannel, abortValue: string): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    this.trySend(encodeCtrl({ t: 'abort', ix, abortValue }), ix) // buffer before unregister so ix is valid
    this.unregister(channel)
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
    this.replayBuffers.set(ix, new ReplayBuffer(this.clientReplayBufferBytes))
    return ix
  }

  private connect(): void {
    if (this.closed) return

    try {
      this.ws = new WebSocket(this.wsUrl)
    } catch (err) {
      this.scheduleReconnect()
      return
    }

    this.ws.binaryType = 'arraybuffer'

    this.ws.onopen = () => {
      this.connected = true
      this.reconnectAttempt = 0
      this.reconnectStart = 0
      this.startReconcile()
      // startPing() is deferred to the 'reconciled' handler — pong deadlines
      // must not run while the server may be blocked processing reconcile.
    }

    this.ws.onmessage = ({ data }: MessageEvent) => {
      const frame = decode(new Uint8Array(data as ArrayBuffer))
      switch (frame.tag) {
        case TAG.CTRL:
          this.handleCtrl(frame.ctrl)
          break
        case TAG.TEXT:
        case TAG.BINARY:
        case TAG.TEXT_ACK_REQ:
          this.handleDataFrame(frame)
          break
        case TAG.ACK_RES: {
          const key = `${frame.index}:${frame.ackedSeq}`
          const pending = this.pendingAcks.get(key)
          if (pending) {
            this.pendingAcks.delete(key)
            pending.resolve(parse(frame.text))
          }
          break
        }
      }
    }

    this.ws.onclose = () => {
      if (this.closed) return
      const wasConnected = this.connected
      this.connected = false
      this.reconciling = false
      this.ws = null
      this.stopPing()
      this.clearTimer('ttl')

      if (!wasConnected && this.reconnectAttempt === 0) {
        // Server rejected the upgrade (e.g. 403) — don't retry.
        this.notifyAllClosed(new ChannelNetworkError('Server rejected WebSocket connection'))
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
    this.reconcileIxes = new Set()
    const open: { id: string; ix: number; lastSeq: number; defer?: boolean }[] = []
    for (const [ix, ch] of this.channels) {
      this.reconcileIxes.add(ix)
      const lastSeq = this.lastSeqByChannel.get(ix) ?? 0
      open.push({ id: ch.id, ix, lastSeq, ...(ch.defer && { defer: true }) })
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

  /** Process a TEXT, BINARY, or TEXT_ACK_REQ data frame — dedup by seq, then deliver. */
  private handleDataFrame(frame: { tag: number; index: number; seq?: number; text?: string; data?: Uint8Array }): void {
    if (frame.seq && !this.trackSeq(frame.index, frame.seq)) return
    if (frame.tag === TAG.TEXT_ACK_REQ) {
      this.channels.get(frame.index)?._onWsAckReqMessage(frame.text!, frame.seq ?? 0)
    } else if (frame.tag === TAG.TEXT) {
      this.channels.get(frame.index)?._onWsMessage(frame.text!)
    } else if (frame.tag === TAG.BINARY) {
      this.channels.get(frame.index)?._onWsBinaryMessage(frame.data!)
    }
  }

  private trySend(frame: Uint8Array<ArrayBuffer>, channelIx: number, seq?: number): void {
    if (this.ws && this.connected && !this.reconciling) {
      this.wireSend(this.ws, frame, channelIx, seq)
    } else {
      this.sendBuffer.push({ frame, channelIx, seq })
    }
  }

  private closeChannel(ix: number, err?: Error): void {
    const ch = this.channels.get(ix)
    if (!ch) return
    this.channels.delete(ix)
    this.channelIndex.delete(ch)
    this.lastSeqByChannel.delete(ix)
    this.replayBuffers.delete(ix)
    ch._onWsClose(err)
    this.clearPendingAcks(ix, err ?? new ChannelClosedError())
    this.startTtlIfIdle()
  }

  // A frame is sent if the server acknowledged its channel in this reconcile round
  // (serverAcknowledgedIxes) OR the channel is still live (this.channels).
  // Close/abort frames for channels the server dropped are skipped —
  // the server already cleaned them up and sending would be a no-op at best.
  private flushSendBuffer(serverAcknowledgedIxes: Set<number>): void {
    const ws = this.ws
    assert(ws)
    const buf = this.sendBuffer
    this.sendBuffer = []
    for (const { frame, channelIx, seq } of buf) {
      if (!this.channels.has(channelIx) && !serverAcknowledgedIxes.has(channelIx)) continue
      this.wireSend(ws, frame, channelIx, seq)
    }
  }

  private wireSend(ws: WebSocket, frame: Uint8Array<ArrayBuffer>, channelIx: number, seq?: number): void {
    if (seq !== undefined) this.replayBuffers.get(channelIx)?.push(seq, frame)
    ws.send(frame)
  }

  private handleCtrl(ctrl: CtrlMessage): void {
    if (!ctrl || typeof ctrl !== 'object') return
    switch (ctrl.t) {
      case 'pong':
        this.resetPongTimer()
        break

      case 'close':
        this.closeChannel(ctrl.ix)
        break

      case 'abort':
        this.closeChannel(ctrl.ix, makeAbortError(parse(ctrl.abortValue)))
        break

      case 'error':
        this.closeChannel(ctrl.ix, makeBugError())
        break

      case 'reconciled': {
        this.closeAbandonedWs()
        if (ctrl.reconnectTimeout) this.reconnectTimeoutMs = ctrl.reconnectTimeout
        if (ctrl.idleTimeout) this.idleTimeoutMs = ctrl.idleTimeout
        if (ctrl.pingInterval) {
          this.pingIntervalMs = ctrl.pingInterval
          this.pongTimeoutMs = ctrl.pingInterval * 2
        }
        if (ctrl.clientReplayBuffer) this.clientReplayBufferBytes = ctrl.clientReplayBuffer
        this.startPing()

        const serverMap = new Map(ctrl.open.map((c) => [c.ix, c.lastSeq]))
        const serverAcknowledgedIxes = new Set(serverMap.keys())
        const reconcileIxes = this.reconcileIxes
        this.reconcileIxes = new Set()
        for (const [ix, ch] of this.channels) {
          if (!reconcileIxes.has(ix)) {
            // Registered after the reconcile was sent — server doesn't know about it yet;
            // leave it in this.channels for the follow-up reconcile below.
            continue
          }
          if (!serverMap.has(ix)) {
            this.channels.delete(ix)
            this.channelIndex.delete(ch)
            this.lastSeqByChannel.delete(ix)
            this.replayBuffers.delete(ix)
            const reconcileErr = new ChannelNetworkError('Channel not acknowledged by server after reconnect')
            ch._onWsClose(reconcileErr)
            this.clearPendingAcks(ix, reconcileErr)
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
        this.flushSendBuffer(serverAcknowledgedIxes)

        // If new channels were registered while the reconcile was in-flight, introduce them now.
        const hasNewChannels = [...this.channels.keys()].some((ix) => !serverAcknowledgedIxes.has(ix))
        if (hasNewChannels) {
          this.startReconcile()
        } else {
          this.startTtlIfIdle()
        }
        break
      }
    }
  }

  // ── Ping / pong ──

  private startPing(): void {
    this.resetPongTimer()
    if (this.pingInterval) return
    this.pingInterval = setInterval(() => {
      if (this.ws && this.connected) this.ws.send(encodeCtrl({ t: 'ping' }))
    }, this.pingIntervalMs)
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
    }, this.pongTimeoutMs)
  }

  private abandonWs(): void {
    const ws = this.ws
    if (!ws) return
    this.ws = null
    this.closeAbandonedWs() // close any previously abandoned WS first
    this.abandonedWs = ws
    ws.onopen = ws.onerror = ws.onclose = null
    // Hot-swap: this connection is potentially stalled, so we open a new
    // one immediately. But we can't be certain it's dead — data may still
    // arrive on it. Keep a data-only handler until the new connection
    // reconciles, at which point closeAbandonedWs() shuts it down.
    // CTRL frames are ignored to avoid interfering with the new connection.
    ws.onmessage = ({ data }: MessageEvent) => {
      const frame = decode(new Uint8Array(data as ArrayBuffer))
      if (frame.tag === TAG.TEXT || frame.tag === TAG.BINARY) {
        this.handleDataFrame(frame)
      }
    }
  }

  private closeAbandonedWs(): void {
    const ws = this.abandonedWs
    if (!ws) return
    this.abandonedWs = null
    ws.onmessage = ws.onclose = null
    try {
      ws.close()
    } catch {
      /* already dead */
    }
  }

  // ── Reconnection ──

  private scheduleReconnect(): void {
    if (this.closed) return
    if (!this.reconnectStart) this.reconnectStart = Date.now()
    if (Date.now() - this.reconnectStart > this.reconnectTimeoutMs) {
      this.notifyAllClosed(new ChannelNetworkError('WebSocket reconnect timed out'))
      return
    }
    const delay = Math.min(WS_RECONNECT_INITIAL_DELAY * 2 ** this.reconnectAttempt, WS_RECONNECT_MAX_DELAY)
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
    }, this.idleTimeoutMs)
  }

  // ── Shutdown ──

  private closeGracefully(): void {
    this.ws?.close(1000)
    this.ws = null
    this.dispose()
  }

  private notifyAllClosed(err?: Error): void {
    for (const ch of this.channels.values()) ch._onWsClose(err)
    this.dispose()
  }

  private dispose(): void {
    this.closed = true
    this.stopPing()
    this.clearTimer('ttl')
    this.clearTimer('reconnectTimer')
    this.closeAbandonedWs()
    this.channels.clear()
    this.channelIndex.clear()
    this.lastSeqByChannel.clear()
    this.replayBuffers.clear()
    this.clearPendingAcks(undefined, new ChannelNetworkError('Connection closed'))
    this.reconcileIxes.clear()
    this.sendBuffer = []
    WsConnection.cache.delete(this.wsUrl)
  }

  private clearPendingAcks(ix: number | undefined, err: Error): void {
    for (const [key, { reject }] of this.pendingAcks) {
      if (ix === undefined || key.startsWith(`${ix}:`)) {
        this.pendingAcks.delete(key)
        reject(err)
      }
    }
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
