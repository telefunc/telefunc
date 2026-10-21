export { WsTransport }

import { assert } from '../../../utils/assert.js'
import { CHANNEL_PING_INTERVAL_MIN_MS, CHANNEL_TRANSPORT, WS_PROBE_TIMEOUT_MS } from '../../constants.js'
import { PieceSender } from '../../flowControl/pieces.js'
import { PagePieceReceiver } from '../../flowControl/client/PagePieceReceiver.js'
import { SendBudget } from '../../flowControl/client/SendBudget.js'
import { Uplink } from '../../flowControl/client/Uplink.js'
import { Downlink } from '../../flowControl/client/Downlink.js'
import {
  LENGTH_BYTES,
  TAG,
  decode,
  encode,
  fixedBytes,
  isOneFrame,
  messageFrames,
  isSequencedTag,
  withTook,
  type DecodedFrame,
  type ReconciledPayload,
} from '../../wire/connectionFrame.js'
import type { ClientConnection, OutboundFrame, ProbeWire, UpgradeTarget } from './connection.js'
import type { Heartbeat } from './Heartbeat.js'

/** What a WebSocket's frames go through, each way. */
type WsEnds = { uplink: Uplink; downlink: Downlink; budget: SendBudget; sender: PieceSender; pieces: PagePieceReceiver }

class WsTransport implements UpgradeTarget {
  readonly type = CHANNEL_TRANSPORT.WS
  readonly sendReconcileOnOpen = true
  readonly reconcileMode = 'release-after-reconciled' as const
  readonly batched = false
  heartbeat: Heartbeat | null = null
  /** The probe's socket and ends, kept as the transport adopts it: the server counts every frame the socket carries. */
  private probed: { ws: WebSocket; ends: WsEnds } | null = null
  private ws: WebSocket | null = null
  /** The wire the page left, still read until the next takes over; its budget's timers may still send. */
  private abandoned: { ws: WebSocket; budget: SendBudget } | null = null
  private connecting = false
  /** The live wire's, set with it by `setupHandlers`: none before the first. */
  private ends: WsEnds | null = null
  private drainCallbacks: Array<() => void> = []
  /** The server's, once a RECONCILED said it. */
  private pingInterval = CHANNEL_PING_INTERVAL_MIN_MS

  private readonly wsUrl: string

  constructor(
    telefuncUrl: string,
    private readonly owner: ClientConnection,
  ) {
    const base = typeof window === 'undefined' ? undefined : window.location.href
    const url = new URL(telefuncUrl, base)
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'
    this.wsUrl = url.href
  }

  async probe(): Promise<ProbeWire | null> {
    let ws: WebSocket
    try {
      ws = new WebSocket(this.wsUrl)
    } catch {
      return null
    }
    ws.binaryType = 'arraybuffer'
    const ends = this.wsEnds(ws)
    const { budget, sender, pieces } = ends

    let onPong: (() => void) | null = null
    let onClose: (() => void) | null = null
    let onFrame: ((frame: DecodedFrame, byteLength: number) => void) | null = null
    const take = (frame: DecodedFrame, byteLength: number, at: number): boolean => {
      if (frame.tag === TAG.PIECES_ACK) return budget.acknowledged(frame)
      if (frame.tag === TAG.PONG) {
        sender.heartbeat(frame.took, at)
        onPong?.()
      } else onFrame?.(frame, byteLength)
      return true
    }
    ws.onmessage = ({ data }: MessageEvent) => {
      if (!receiveMessage(pieces, new Uint8Array(data as ArrayBuffer), performance.now(), take)) ws.close()
    }
    ws.onclose = () => {
      budget.close()
      if (this.probed?.ws === ws) this.probed = null
      onClose?.()
    }
    ws.onerror = () => {}
    ws.onopen = () => sender.send(encode.ping())

    // First pong proves the wire is alive — consumer reassigns onPong/onClose after the await.
    const ready = await new Promise<boolean>((resolve) => {
      const timer = setTimeout(() => resolve(false), WS_PROBE_TIMEOUT_MS)
      onPong = () => {
        clearTimeout(timer)
        resolve(true)
      }
      onClose = () => {
        clearTimeout(timer)
        resolve(false)
      }
    })
    if (!ready) {
      budget.close()
      try {
        ws.close()
      } catch {}
      return null
    }

    this.probed = { ws, ends }
    // A RECONCILED may have come while it waited for the PONG.
    sender.setPingInterval(this.pingInterval)
    return {
      ping: () => {
        try {
          sender.send(withTook(encode.ping(), pieces.took))
        } catch {}
      },
      onPong: (cb) => {
        onPong = cb
      },
      onClose: (cb) => {
        onClose = cb
      },
      send: (frame) => {
        try {
          sender.send(frame)
        } catch {
          // Socket died between the open event and this send — `onClose` aborts the attempt.
        }
      },
      onFrame: (cb) => {
        onFrame = cb
      },
      close: () => {
        if (this.probed?.ws === ws) this.probed = null
        budget.close()
        try {
          ws.close()
        } catch {}
      },
      uplink: ends.uplink,
    }
  }

  adoptProbe(): void {
    const probed = this.probed
    assert(probed !== null)
    this.probed = null
    this.ws = probed.ws
    this.connecting = false
    this.setupHandlers(probed.ws, probed.ends)
  }

  start(): void {
    if (this.connecting || this.hasWire()) return

    this.connecting = true

    let ws: WebSocket
    try {
      ws = new WebSocket(this.wsUrl)
    } catch {
      this.connecting = false
      this.owner._onTransportClosed(this)
      return
    }

    this.ws = ws
    ws.binaryType = 'arraybuffer'

    ws.onopen = () => {
      if (this.ws !== ws) return
      this.handleOpen(ws)
    }

    this.setupHandlers(ws, this.wsEnds(ws))
  }

  private handleOpen(ws: WebSocket): void {
    if (this.ws !== ws) return
    this.connecting = false
    this.owner._onTransportOpen(this)
  }

  attachHeartbeat(hb: Heartbeat): void {
    this.heartbeat = hb
    // What the server announced comes at the rate it arrives at; a PONG, behind what the page announced.
    hb.expectFrom(() =>
      Math.max(this.ends?.pieces.expectedBy(hb.pongTimeoutMs) ?? 0, this.ends?.sender.acknowledgedBy ?? 0),
    )
  }

  detachHeartbeat(): void {
    this.heartbeat?.stop()
    this.heartbeat = null
  }

  drained(): Promise<void> {
    // A socket sends what it buffered before its close, but not what its piece sender holds for room.
    if (!this.ws || !this.ends?.sender.heldBytes) return Promise.resolve()
    return new Promise((resolve) => this.drainCallbacks.push(resolve))
  }

  private settleDrained(): void {
    for (const cb of this.drainCallbacks.splice(0)) cb()
  }

  /** Every frame `ws` carries goes through its budget's sender, which counts the bytes its receiver acknowledges. */
  private wsEnds(ws: WebSocket): WsEnds {
    const uplink = new Uplink()
    const downlink = new Downlink(uplink)
    const budget = new SendBudget({
      transmit: (message) => ws.send(message),
      queued: () => ws.bufferedAmount,
      pingInterval: this.pingInterval,
      uplink,
      downlink,
      onRoom: () => {
        if (this.ws !== ws) return
        sender.sendHeld()
        this.onRoom(sender)
      },
    })
    const { sender } = budget
    return {
      uplink,
      downlink,
      budget,
      sender,
      pieces: new PagePieceReceiver({
        send: (frame) => sender.send(frame),
        carry: (ack) => sender.carry(ack),
        uplink,
      }),
    }
  }

  private setupHandlers(ws: WebSocket, ends: WsEnds): void {
    const { downlink, budget, sender, pieces } = ends
    this.ends = ends
    const take = (frame: DecodedFrame, byteLength: number, at: number): boolean => {
      if (frame.tag === TAG.PIECES_ACK) {
        if (!budget.acknowledged(frame)) return false
        this.onRoom(sender)
      } else if (frame.tag === TAG.PONG) {
        sender.heartbeat(frame.took, at)
        this.heartbeat?.resetPong()
        this.owner._onTransportPong(frame.ended)
      } else this.owner._onTransportFrame(frame, this, byteLength, ws === this.abandoned?.ws)
      return true
    }
    ws.onmessage = ({ data }: MessageEvent) => {
      const now = performance.now()
      const raw = new Uint8Array(data as ArrayBuffer)
      this.heartbeat?.noteReceived(raw.byteLength, now)
      downlink.delivered(raw.byteLength, now)
      budget.received()
      if (!receiveMessage(pieces, raw, now, take)) ws.close()
    }
    ws.onclose = () => {
      budget.close()
      if (this.ws === ws) this.ws = null
      this.connecting = false
      this.settleDrained()
      this.owner._onTransportClosed(this)
    }
    ws.onerror = () => {}
  }

  /** What its sender held may have gone. */
  private onRoom(sender: PieceSender): void {
    if (sender.heldBytes === 0) this.settleDrained()
    this.owner._onTransportRoom(this)
  }

  hasWire(): boolean {
    return this.ws !== null
  }

  isConnecting(): boolean {
    return this.connecting
  }

  sendFrame(frame: OutboundFrame): void {
    assert(this.ws && this.ends)
    if (frame.kind === 'acknowledgement') this.ends.sender.carry(() => frame.frame)
    else this.ends.budget.send(frame.frame)
  }

  sendRoom(): number {
    assert(this.ends)
    return this.ends.budget.sendRoom
  }

  get uplink(): Uplink {
    assert(this.ends)
    return this.ends.uplink
  }

  get downlink(): Downlink {
    assert(this.ends)
    return this.ends.downlink
  }

  /** What its socket holds unsent, and what its piece sender hasn't handed it. */
  bufferedAmount(): number {
    return (this.ws?.bufferedAmount ?? 0) + (this.ends?.sender.unhandedBytes ?? 0)
  }

  abandonActiveTransport(): void {
    const ws = this.ws
    if (!ws) return
    this.ws = null
    this.closeAbandonedTransport()
    assert(this.ends)
    const { budget } = this.ends
    this.abandoned = { ws, budget }
    ws.onopen = ws.onerror = null
    ws.onclose = () => budget.close()
    this.settleDrained()
  }

  closeAbandonedTransport(): void {
    if (!this.abandoned) return
    const { ws, budget } = this.abandoned
    this.abandoned = null
    ws.onmessage = ws.onclose = null
    budget.close()
    try {
      ws.close()
    } catch {}
  }

  applyReconciledSettings(ctrl: ReconciledPayload): void {
    assert(this.ends) // a RECONCILED came on its wire
    this.pingInterval = ctrl.pingInterval
    this.ends.sender.setPingInterval(this.pingInterval)
    this.probed?.ends.sender.setPingInterval(this.pingInterval)
    // The server sends nothing on a wire before the page's first frame arrived: that frame crossed by now.
    this.ends.sender.answered(performance.now())
  }

  sendPing(frame: Uint8Array<ArrayBuffer>): void {
    if (this.ws?.readyState !== WebSocket.OPEN) return
    assert(this.ends)
    this.ends.sender.send(withTook(frame, this.ends.pieces.took))
  }

  dispose(): void {
    this.connecting = false
    // A socket sends what it was handed before its close.
    this.probed?.ends.sender.flush()
    this.probed?.ends.budget.close()
    if (this.ws) this.ends?.sender.flush()
    if (this.ws) this.ends?.budget.close()
    const wsProbed = this.probed?.ws
    this.probed = null
    if (wsProbed) {
      wsProbed.onopen = wsProbed.onmessage = wsProbed.onerror = wsProbed.onclose = null
      try {
        wsProbed.close(1000)
      } catch {}
    }
    const ws = this.ws
    this.ws = null
    if (ws) {
      ws.onopen = ws.onmessage = ws.onerror = ws.onclose = null
      try {
        ws.close(1000)
      } catch {}
    }
    this.closeAbandonedTransport()
    this.settleDrained()
  }
}

/** Hands `take` the frames of a WebSocket message that arrived at `at`; false where its wire closes. */
function receiveMessage(
  pieces: PagePieceReceiver,
  raw: Uint8Array<ArrayBuffer>,
  at: number,
  take: (frame: DecodedFrame, byteLength: number, at: number) => boolean,
): boolean {
  // The frames before a message's last ride on it: each taken as it would be alone, but not dated apart from the last.
  const riding: { frame: DecodedFrame; bytes: number }[] = []
  let last: DecodedFrame | null = null
  let lastBytes = 0
  try {
    const frames = isOneFrame(raw) ? [raw] : Array.from(messageFrames(raw))
    const own = frames.pop()!
    for (const each of frames) {
      const frame = decode(each)
      pieces.arrived(each.byteLength, isSequencedTag(frame.tag), at, 'riding')
      if (fixedBytes(frame.tag) === 0) pieces.tookLength(LENGTH_BYTES)
      riding.push({ frame, bytes: each.byteLength })
    }
    const frame = decode(own)
    if (frame.tag === TAG.ANNOUNCE) pieces.announce(frame.bytes, at, frame.floor)
    else if (frame.tag === TAG.PIECE) {
      const whole = pieces.add(frame.total, frame.piece, at)
      if (whole !== null) {
        last = decode(whole)
        lastBytes = whole.byteLength
      }
    } else {
      pieces.arrived(own.byteLength, isSequencedTag(frame.tag), at, 'alone')
      last = frame
      lastBytes = own.byteLength
    }
  } catch {
    return false
  }
  for (const { frame, bytes } of riding) if (!take(frame, bytes, at)) return false
  return last === null || take(last, lastBytes, at)
}
