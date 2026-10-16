export { WsTransport }

import { assert } from '../../utils/assert.js'
import { CHANNEL_PING_INTERVAL_MIN_MS, CHANNEL_TRANSPORT, WS_PROBE_TIMEOUT_MS } from '../constants.js'
import { PieceSender } from '../pieces.js'
import { PagePieceReceiver } from './page-piece-receiver.js'
import { SendBudget } from './send-budget.js'
import { Uplink } from './uplink.js'
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
} from '../shared-ws.js'
import type { ClientConnection, OutboundFrame, ProbeWire, UpgradeTarget } from './connection.js'
import type { Heartbeat } from './heartbeat.js'

/** What a WebSocket's frames go through, each way. */
type WsEnds = { uplink: Uplink; budget: SendBudget; pieces: PagePieceReceiver }

class WsTransport implements UpgradeTarget {
  readonly type = CHANNEL_TRANSPORT.WS
  readonly sendReconcileOnOpen = true
  readonly reconcileMode = 'release-after-reconciled' as const
  readonly batched = false
  heartbeat: Heartbeat | null = null
  /** The probe's socket, with the ends it keeps as the transport adopts it: the server counts every frame the socket
   *  carries. */
  private probed: { ws: WebSocket; ends: WsEnds } | null = null
  private ws: WebSocket | null = null
  private abandonedWs: WebSocket | null = null
  private connecting = false
  private everOpened = false
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
    const { budget, pieces } = ends
    const { sender } = budget

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
    this.everOpened = true
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
    this.everOpened = true
    this.connecting = false
    this.owner._onTransportOpen(this)
  }

  attachHeartbeat(hb: Heartbeat): void {
    this.heartbeat = hb
    // What the server announced comes at the rate it arrives at; a PONG, behind what the page announced.
    hb.expectFrom(() =>
      Math.max(this.ends?.pieces.expectedBy(hb.pongTimeoutMs) ?? 0, this.ends?.budget.sender.acknowledgedBy ?? 0),
    )
  }

  detachHeartbeat(): void {
    this.heartbeat?.stop()
    this.heartbeat = null
  }

  drained(): Promise<void> {
    // A socket sends what it buffered before its close, but not what its piece sender holds for room.
    if (!this.ws || !this.ends?.budget.sender.heldBytes) return Promise.resolve()
    return new Promise((resolve) => this.drainCallbacks.push(resolve))
  }

  private settleDrained(): void {
    for (const cb of this.drainCallbacks.splice(0)) cb()
  }

  /** Every frame `ws` carries goes through its budget's sender, which counts the bytes its receiver acknowledges. */
  private wsEnds(ws: WebSocket): WsEnds {
    const uplink = new Uplink()
    const budget = new SendBudget(
      (message) => ws.send(message),
      () => ws.bufferedAmount,
      this.pingInterval,
      uplink,
      () => {
        if (this.ws !== ws) return
        sender.sendHeld()
        this.onRoom(sender)
      },
    )
    const { sender } = budget
    return {
      uplink,
      budget,
      pieces: new PagePieceReceiver(
        (frame) => sender.send(frame),
        (ack) => sender.carry(ack),
        uplink,
      ),
    }
  }

  private setupHandlers(ws: WebSocket, ends: WsEnds): void {
    const { uplink, budget, pieces } = ends
    const { sender } = budget
    this.ends = ends
    const take = (frame: DecodedFrame, byteLength: number, at: number): boolean => {
      if (frame.tag === TAG.PIECES_ACK) {
        if (!budget.acknowledged(frame)) return false
        this.onRoom(sender)
      } else if (frame.tag === TAG.PONG) {
        sender.heartbeat(frame.took, at)
        this.heartbeat?.resetPong()
        this.owner._onTransportPong(frame.ended)
      } else this.owner._onTransportFrame(frame, this, byteLength, ws === this.abandonedWs)
      return true
    }
    ws.onmessage = ({ data }: MessageEvent) => {
      const now = performance.now()
      const raw = new Uint8Array(data as ArrayBuffer)
      this.heartbeat?.noteReceived(raw.byteLength, now)
      uplink.delivered(raw.byteLength, now)
      budget.received()
      if (!receiveMessage(pieces, raw, now, take)) ws.close()
    }
    ws.onclose = () => {
      if (this.ws === ws) this.ws = null
      this.connecting = false
      this.settleDrained()
      this.owner._onTransportClosed(this, { rejectedByServer: !this.everOpened })
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
    if (frame.kind === 'acknowledgement') this.ends.budget.sender.carry(() => frame.frame)
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

  /** What its socket holds unsent, and what its piece sender hasn't handed it. */
  bufferedAmount(): number {
    return (this.ws?.bufferedAmount ?? 0) + (this.ends?.budget.sender.unhandedBytes ?? 0)
  }

  abandonActiveTransport(): void {
    const ws = this.ws
    if (!ws) return
    this.ws = null
    this.closeAbandonedTransport()
    this.abandonedWs = ws
    ws.onopen = ws.onerror = ws.onclose = null
    this.settleDrained()
  }

  closeAbandonedTransport(): void {
    const ws = this.abandonedWs
    if (!ws) return
    this.abandonedWs = null
    ws.onmessage = ws.onclose = null
    try {
      ws.close()
    } catch {}
  }

  applyReconciledSettings(ctrl: ReconciledPayload): void {
    assert(this.ends) // a RECONCILED came on its wire
    this.pingInterval = ctrl.pingInterval
    this.ends.budget.sender.setPingInterval(this.pingInterval)
    this.probed?.ends.budget.sender.setPingInterval(this.pingInterval)
    // The server sends nothing on a wire before the page's first frame arrived: that frame crossed by now.
    this.ends.budget.sender.answered(performance.now())
  }

  sendPing(frame: Uint8Array<ArrayBuffer>): void {
    if (this.ws?.readyState !== WebSocket.OPEN) return
    assert(this.ends)
    this.ends.budget.sender.send(withTook(frame, this.ends.pieces.took))
  }

  dispose(): void {
    this.connecting = false
    // A socket sends what it was handed before its close.
    this.probed?.ends.budget.sender.flush()
    if (this.ws) this.ends?.budget.sender.flush()
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
  let riding: { frame: DecodedFrame; bytes: number }[] | undefined
  let last: DecodedFrame | null = null
  let lastBytes = 0
  try {
    const frames = isOneFrame(raw) ? null : Array.from(messageFrames(raw))
    const own = frames === null ? raw : frames.pop()!
    riding = frames?.map((each) => {
      const frame = decode(each)
      pieces.arrived(each.byteLength, isSequencedTag(frame.tag), at, true)
      if (fixedBytes(frame.tag) === 0) pieces.tookLength(LENGTH_BYTES)
      return { frame, bytes: each.byteLength }
    })
    const frame = decode(own)
    if (frame.tag === TAG.ANNOUNCE) pieces.announce(frame.bytes, at, frame.floor)
    else if (frame.tag === TAG.PIECE) {
      const whole = pieces.add(frame.total, frame.piece, at)
      if (whole !== null) {
        last = decode(whole)
        lastBytes = whole.byteLength
      }
    } else {
      pieces.arrived(own.byteLength, isSequencedTag(frame.tag), at, false)
      last = frame
      lastBytes = own.byteLength
    }
  } catch {
    return false
  }
  if (riding !== undefined) {
    for (const { frame, bytes } of riding) if (!take(frame, bytes, at)) return false
  }
  return last === null || take(last, lastBytes, at)
}
