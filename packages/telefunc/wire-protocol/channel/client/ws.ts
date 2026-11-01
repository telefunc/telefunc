export { WsTransport }

import { assert } from '../../../utils/assert.js'
import { CHANNEL_PING_INTERVAL_MIN_MS, CHANNEL_TRANSPORT, WS_PROBE_TIMEOUT_MS } from '../../constants.js'
import { PieceSender } from '../../flowControl/pieces.js'
import { ClientHeadroom, type ClientHeadroomOwner } from '../../flowControl/client/headroom.js'
import { ClientPieceReceiver } from '../../flowControl/client/pieceReceiver.js'
import { Uplink } from '../../flowControl/client/Uplink.js'
import { Downlink } from '../../flowControl/client/Downlink.js'
import { decode, encode, isOneFrame, messageFrames, withTook, type DecodedFrame } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import type { ReconciledPayload } from '../../wire/payloads.js'
import type { OutboundFrame, TransportOwner } from './connection.js'
import type { ProbeOwner, ProbeWire, UpgradeTarget } from './Upgrade.js'
import type { Heartbeat } from './Heartbeat.js'

/** What a WebSocket's frames go through, each way; the server's `WirePieces` holds its headroom and receiver. */
type WirePieces = {
  uplink: Uplink
  downlink: Downlink
  headroom: ClientHeadroom
  sender: PieceSender
  receiver: ClientPieceReceiver
}

type WsTransportOptions = {
  telefuncUrl: string
  owner: TransportOwner
}

class WsTransport implements UpgradeTarget, ClientHeadroomOwner {
  readonly type = CHANNEL_TRANSPORT.WS
  readonly carriesReconcileInOpen = false
  readonly batched = false
  heartbeat: Heartbeat | null = null
  /** The probe's socket and pieces, kept as the transport adopts it: the server counts every frame the socket carries. */
  private probed: { ws: WebSocket; pieces: WirePieces } | null = null
  private ws: WebSocket | null = null
  /** The wire the page left, still read until the next takes over; its headroom's timers may still send. */
  private abandoned: { ws: WebSocket; headroom: ClientHeadroom } | null = null
  private connecting = false
  /** The live wire's, set with it by `setupHandlers`: none before the first. */
  private pieces: WirePieces | null = null
  private drainCallbacks: Array<() => void> = []
  /** The server's, once a RECONCILED said it. */
  private pingInterval = CHANNEL_PING_INTERVAL_MIN_MS

  private readonly wsUrl: string
  private readonly owner: TransportOwner

  constructor({ telefuncUrl, owner }: WsTransportOptions) {
    this.owner = owner
    const base = typeof window === 'undefined' ? undefined : window.location.href
    const url = new URL(telefuncUrl, base)
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'
    this.wsUrl = url.href
  }

  async probe(owner: ProbeOwner): Promise<ProbeWire | null> {
    let ws: WebSocket
    try {
      ws = new WebSocket(this.wsUrl)
    } catch {
      return null
    }
    ws.binaryType = 'arraybuffer'
    const pieces = this.wirePieces(ws)
    const { headroom, sender, receiver } = pieces

    // Till its first PONG proves the wire, `proved` settles the wait below; from then on it tells `owner`, till closed.
    let proved: ((alive: boolean) => void) | null = null
    let reporting = false
    const take = (frame: DecodedFrame, byteLength: number, at: number): boolean => {
      if (frame.tag === TAG.PIECES_ACK) return headroom.acknowledged(frame)
      if (frame.tag === TAG.PONG) {
        sender.heartbeat(frame.took, at)
        if (reporting) owner.onProbePong()
        else proved?.(true)
      } else if (reporting) owner.onProbeFrame(frame, byteLength)
      return true
    }
    ws.onmessage = ({ data }: MessageEvent) => {
      if (!receiveMessage(receiver, new Uint8Array(data as ArrayBuffer), performance.now(), take)) ws.close()
    }
    ws.onclose = () => {
      headroom.close()
      if (this.probed?.ws === ws) this.probed = null
      if (reporting) owner.onProbeClosed()
      else proved?.(false)
    }
    ws.onerror = () => {}
    ws.onopen = () => sender.send(encode.ping(), 0)

    const ready = await new Promise<boolean>((resolve) => {
      const timer = setTimeout(() => resolve(false), WS_PROBE_TIMEOUT_MS)
      proved = (alive) => {
        clearTimeout(timer)
        resolve(alive)
      }
    })
    if (!ready) {
      headroom.close()
      try {
        ws.close()
      } catch {}
      return null
    }

    this.probed = { ws, pieces }
    // A RECONCILED may have come while it waited for the PONG.
    sender.setPingInterval(this.pingInterval)
    reporting = true
    return {
      ping: () => {
        try {
          sender.send(withTook(encode.ping(), receiver.took), 0)
        } catch {}
      },
      send: (frame) => {
        try {
          sender.send(frame, 0)
        } catch {
          // Socket died between the open event and this send — `onProbeClosed` aborts the attempt.
        }
      },
      close: () => {
        reporting = false
        if (this.probed?.ws === ws) this.probed = null
        headroom.close()
        try {
          ws.close()
        } catch {}
      },
      uplink: pieces.uplink,
    }
  }

  adoptProbe(): void {
    const probed = this.probed
    assert(probed !== null)
    this.probed = null
    this.ws = probed.ws
    this.connecting = false
    this.setupHandlers(probed.ws, probed.pieces)
  }

  start(): void {
    if (this.connecting || this.hasWire()) return

    this.connecting = true

    let ws: WebSocket
    try {
      ws = new WebSocket(this.wsUrl)
    } catch {
      this.connecting = false
      this.owner.onTransportClosed(this)
      return
    }

    this.ws = ws
    ws.binaryType = 'arraybuffer'

    ws.onopen = () => {
      if (this.ws !== ws) return
      this.handleOpen(ws)
    }

    this.setupHandlers(ws, this.wirePieces(ws))
  }

  private handleOpen(ws: WebSocket): void {
    if (this.ws !== ws) return
    this.connecting = false
    this.owner.onTransportOpen(this)
  }

  attachHeartbeat(hb: Heartbeat): void {
    this.heartbeat = hb
    // What the server announced comes at the rate it arrives at; a PONG, behind what the page announced.
    hb.expectFrom(() =>
      Math.max(this.pieces?.receiver.expectedBy(hb.pongTimeoutMs) ?? 0, this.pieces?.sender.acknowledgedBy ?? 0),
    )
  }

  detachHeartbeat(): void {
    this.heartbeat?.stop()
    this.heartbeat = null
  }

  drained(): Promise<void> {
    // A socket sends what it buffered before its close, but not what waits in its piece sender for headroom.
    if (!this.ws || !this.pieces?.sender.waitingBytes) return Promise.resolve()
    return new Promise((resolve) => this.drainCallbacks.push(resolve))
  }

  private settleDrained(): void {
    for (const cb of this.drainCallbacks.splice(0)) cb()
  }

  /** Every frame `ws` carries goes through its headroom's sender, which counts the bytes its receiver acknowledges. */
  private wirePieces(ws: WebSocket): WirePieces {
    const uplink = new Uplink()
    const downlink = new Downlink(uplink)
    const headroom = new ClientHeadroom({
      socket: ws,
      pingInterval: this.pingInterval,
      uplink,
      downlink,
      owner: this,
    })
    return {
      uplink,
      downlink,
      headroom,
      sender: headroom.sender,
      receiver: new ClientPieceReceiver({ owner: headroom }),
    }
  }

  private setupHandlers(ws: WebSocket, pieces: WirePieces): void {
    const { downlink, headroom, sender, receiver } = pieces
    this.pieces = pieces
    const take = (frame: DecodedFrame, byteLength: number, at: number): boolean => {
      if (frame.tag === TAG.PIECES_ACK) {
        if (!headroom.acknowledged(frame)) return false
        this.headroomChanged(sender)
      } else if (frame.tag === TAG.PONG) {
        sender.heartbeat(frame.took, at)
        this.heartbeat?.resetPong()
        this.owner.onTransportPong(frame.ended)
      } else this.owner.onTransportFrame(frame, this, byteLength, ws === this.abandoned?.ws)
      return true
    }
    ws.onmessage = ({ data }: MessageEvent) => {
      const now = performance.now()
      const raw = new Uint8Array(data as ArrayBuffer)
      this.heartbeat?.noteReceived(raw.byteLength, now)
      downlink.delivered(raw.byteLength, now)
      headroom.received()
      if (!receiveMessage(receiver, raw, now, take)) ws.close()
    }
    ws.onclose = () => {
      headroom.close()
      if (this.ws === ws) this.ws = null
      this.connecting = false
      this.settleDrained()
      this.owner.onTransportClosed(this)
    }
    ws.onerror = () => {}
  }

  /** `ClientHeadroomOwner`: the live wire's sender sends what waited. */
  onHeadroom(headroom: ClientHeadroom): void {
    if (this.ws === null || this.pieces?.headroom !== headroom) return
    headroom.sendWaiting()
    this.headroomChanged(headroom.sender)
  }

  /** What waited in its sender may have gone. */
  private headroomChanged(sender: PieceSender): void {
    if (sender.waitingBytes === 0) this.settleDrained()
    this.owner.onTransportHeadroom(this)
  }

  hasWire(): boolean {
    return this.ws !== null
  }

  isConnecting(): boolean {
    return this.connecting
  }

  sendFrame(frame: OutboundFrame): void {
    assert(this.ws && this.pieces)
    if (frame.kind === 'acknowledgement') this.pieces.sender.carry(() => frame.frame)
    else this.pieces.headroom.send(frame.frame)
  }

  headroom(): number {
    assert(this.pieces)
    return this.pieces.headroom.headroomPastSent()
  }

  get uplink(): Uplink {
    assert(this.pieces)
    return this.pieces.uplink
  }

  get downlink(): Downlink {
    assert(this.pieces)
    return this.pieces.downlink
  }

  /** What its socket holds unsent, and what its piece sender hasn't handed it. */
  bufferedAmount(): number {
    return (this.ws?.bufferedAmount ?? 0) + (this.pieces?.sender.unhandedBytes ?? 0)
  }

  abandonActiveTransport(): void {
    const ws = this.ws
    if (!ws) return
    this.ws = null
    this.closeAbandonedTransport()
    assert(this.pieces)
    const { headroom } = this.pieces
    this.abandoned = { ws, headroom }
    ws.onopen = ws.onerror = null
    ws.onclose = () => headroom.close()
    this.settleDrained()
  }

  closeAbandonedTransport(): void {
    if (!this.abandoned) return
    const { ws, headroom } = this.abandoned
    this.abandoned = null
    ws.onmessage = ws.onclose = null
    headroom.close()
    try {
      ws.close()
    } catch {}
  }

  applyReconciledSettings(ctrl: ReconciledPayload): void {
    assert(this.pieces) // a RECONCILED came on its wire
    this.pingInterval = ctrl.pingInterval
    this.pieces.sender.setPingInterval(this.pingInterval)
    this.probed?.pieces.sender.setPingInterval(this.pingInterval)
    // The server sends nothing on a wire before the page's first frame arrived: that frame crossed by now.
    this.pieces.sender.answered(performance.now())
  }

  sendPing(frame: Uint8Array<ArrayBuffer>): void {
    if (this.ws?.readyState !== WebSocket.OPEN) return
    assert(this.pieces)
    this.pieces.sender.send(withTook(frame, this.pieces.receiver.took), 0)
  }

  dispose(): void {
    this.connecting = false
    // A socket sends what it was handed before its close.
    this.probed?.pieces.sender.flush()
    this.probed?.pieces.headroom.close()
    if (this.ws) this.pieces?.sender.flush()
    if (this.ws) this.pieces?.headroom.close()
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
  receiver: ClientPieceReceiver,
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
      receiver.arrived(frame.tag, each.byteLength, at, 'riding')
      riding.push({ frame, bytes: each.byteLength })
    }
    const frame = decode(own)
    if (frame.tag === TAG.ANNOUNCE) receiver.announce(frame.bytes, at, frame.floor)
    else if (frame.tag === TAG.PIECE) {
      const whole = receiver.add(frame.total, frame.piece, at)
      if (whole !== null) {
        last = decode(whole)
        lastBytes = whole.byteLength
      }
    } else {
      receiver.arrived(frame.tag, own.byteLength, at, 'alone')
      last = frame
      lastBytes = own.byteLength
    }
  } catch {
    return false
  }
  for (const { frame, bytes } of riding) if (!take(frame, bytes, at)) return false
  return last === null || take(last, lastBytes, at)
}
