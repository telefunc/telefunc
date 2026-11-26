export { WsTransport }

import { assert } from '../../../utils/assert.js'
import {
  CHANNEL_PING_INTERVAL_MIN_MS,
  CHANNEL_TRANSPORT,
  LOSSY_CONFIRM_SLACK_MS,
  WIRE_QUEUE_DELAY_MS,
  WS_CLOSE_PAGE_LEFT,
  WS_CLOSE_REFUSED,
  WS_CLOSE_WIRE_VERSION,
  WS_PROBE_TIMEOUT_MS,
} from '../../constants.js'
import { PieceSender } from '../../flowControl/pieces.js'
import { ClientHeadroom, type ClientHeadroomOwner } from '../../flowControl/client/ClientHeadroom.js'
import { ClientPieceReceiver } from '../../flowControl/client/ClientPieceReceiver.js'
import { Uplink } from '../../flowControl/client/Uplink.js'
import { Downlink } from '../../flowControl/client/Downlink.js'
import {
  decode,
  encode,
  isOneFrame,
  messageFrames,
  stampWindowHold,
  withTook,
  type DecodedFrame,
} from '../../wire/frame.js'
import { TAG, isSequencedTag } from '../../wire/tags.js'
import { readU16 } from '../../wire/header.js'
import { pairPublishes } from './pairPublishes.js'
import type { ReconciledPayload } from '../../wire/payloads.js'
import type { OutboundFrame } from './ClientConnection.js'
import type { TransportOwner } from './WireLifecycle.js'
import type { ProbeOwner, ProbeWire, UpgradeTarget } from './Upgrade.js'
import type { Heartbeat } from './Heartbeat.js'
import { getWireUrl } from './getSessionUrl.js'
import { endOfWire } from './readAnswer.js'

/** What a WebSocket's frames go through, each way; the server's `WirePieces` holds its headroom and receiver. */
type WirePieces = {
  uplink: Uplink
  downlink: Downlink
  headroom: ClientHeadroom
  sender: PieceSender
  receiver: ClientPieceReceiver
  pair: ReturnType<typeof pairPublishes>
}

/** The socket of the attempt under way, not open yet, with what its frames go through once it is the wire. */
type Connecting = { ws: WebSocket; pieces: WirePieces; startedAt: number }

type WsTransportOptions = {
  telefuncUrl: string
  owner: TransportOwner
}

class WsTransport implements UpgradeTarget, ClientHeadroomOwner {
  readonly type = CHANNEL_TRANSPORT.WS
  readonly carriesReconcileInOpen = false
  readonly batched = false
  heartbeat: Heartbeat | null = null
  /** The probe its PONG proved, till the transport adopts it or it ends: the server counts every frame its socket
   *  carries, so adoption keeps its pieces. */
  private probed: PendingProbe | null = null
  private ws: WebSocket | null = null
  /** The wire the page left, still read until the next takes over; its headroom's timers may still send. */
  private abandoned: { ws: WebSocket; headroom: ClientHeadroom } | null = null
  /** The attempt's socket, not open yet. */
  private connecting: Connecting | null = null
  /** The live wire's, set with it by `setupHandlers`: none before the first, nor once disposed. */
  private pieces: WirePieces | null = null
  private drainCallbacks: Array<() => void> = []
  private firstPing: 'waiting' | { frame: Uint8Array<ArrayBuffer>; timer: ReturnType<typeof setTimeout> } | null = null
  /** The server's, once a RECONCILED said it. */
  private pingInterval = CHANNEL_PING_INTERVAL_MIN_MS

  private readonly telefuncUrl: string
  private readonly owner: TransportOwner

  constructor({ telefuncUrl, owner }: WsTransportOptions) {
    this.owner = owner
    this.telefuncUrl = telefuncUrl
    // React Native has a `window` without `addEventListener`.
    globalThis.addEventListener?.('pagehide', this.onPageHide)
  }

  /** Read at each connect: the session's URL names its home once the server named it. */
  private wsUrl(): string {
    const base = typeof window === 'undefined' ? undefined : window.location.href
    const url = new URL(getWireUrl(this.telefuncUrl), base)
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'
    // A WebSocket URL may have no fragment.
    url.hash = ''
    return url.href
  }

  /** A page that leaves for good says so in its sockets' close code, and opens none it was opening; one kept in the
   *  back/forward cache keeps them. */
  private readonly onPageHide = (event: PageTransitionEvent): void => {
    if (event.persisted) return
    this.letGoConnecting()
    try {
      this.ws?.close(WS_CLOSE_PAGE_LEFT)
    } catch {}
    this.probed?.pageLeft()
  }

  async probe(owner: ProbeOwner): Promise<ProbeWire | null> {
    let ws: WebSocket
    try {
      ws = new WebSocket(this.wsUrl())
    } catch {
      return null
    }
    ws.binaryType = 'arraybuffer'
    const probe = new PendingProbe(ws, this.wirePieces(ws), owner, (ended) => {
      if (this.probed === ended) this.probed = null
    })
    if (!(await probe.proven())) return null
    this.probed = probe
    // A RECONCILED may have come while it waited for the PONG.
    probe.setPingInterval(this.pingInterval)
    return probe
  }

  adoptProbe(): void {
    const probed = this.probed
    assert(probed !== null)
    this.probed = null
    const { ws, pieces } = probed.handOver()
    this.ws = ws
    this.setupHandlers(ws, pieces)
  }

  start(): void {
    if (this.connecting || this.hasWire()) return
    let ws: WebSocket
    try {
      ws = new WebSocket(this.wsUrl())
    } catch {
      this.owner.onTransportClosed(this)
      return
    }
    ws.binaryType = 'arraybuffer'
    const socket: Connecting = { ws, pieces: this.wirePieces(ws), startedAt: performance.now() }
    this.connecting = socket
    this.pieces = socket.pieces
    ws.onopen = () => this.adopt(socket)
    // Its first `error` or `close`: Node 22's undici 6 fires no `close` before the upgrade (nodejs/undici#3506).
    ws.onclose = ws.onerror = () => this.connectFailed(socket)
  }

  private adopt(socket: Connecting): void {
    this.connecting = null
    this.ws = socket.ws
    this.setupHandlers(socket.ws, socket.pieces)
    this.owner.onTransportAnswered(this)
    this.owner.onTransportOpen(this, socket.startedAt)
  }

  /** The socket failed before it opened, once. */
  private connectFailed(socket: Connecting): void {
    socket.ws.onclose = socket.ws.onerror = null
    socket.pieces.headroom.close()
    this.connecting = null
    this.owner.onTransportClosed(this)
  }

  cancelAttempt(): void {
    this.letGoConnecting()
    if (!this.ws) return
    assert(this.pieces)
    this.letGo({ ws: this.ws, pieces: this.pieces })
    this.ws = null
    this.settleDrained()
  }

  private letGoConnecting(): void {
    this.letGo(this.connecting)
    this.connecting = null
  }

  /** A socket the attempt no longer needs: closed, and closed again should it open all the same, before any frame. */
  private letGo(socket: { ws: WebSocket; pieces: WirePieces } | null): void {
    if (!socket) return
    const { ws } = socket
    ws.onopen = () => ws.close()
    ws.onmessage = ws.onclose = ws.onerror = null
    socket.pieces.headroom.close()
    try {
      ws.close()
    } catch {}
  }

  attachHeartbeat(heartbeat: Heartbeat): void {
    this.heartbeat = heartbeat
    this.firstPing = 'waiting'
    // What the server announced comes at the rate it arrives at; a PONG, behind what the page announced.
    heartbeat.expectFrom(() =>
      Math.max(this.pieces?.receiver.expectedBy(heartbeat.pongTimeoutMs) ?? 0, this.pieces?.sender.acknowledgedBy ?? 0),
    )
  }

  detachHeartbeat(): void {
    this.heartbeat?.stop()
    this.heartbeat = null
    if (this.firstPing && this.firstPing !== 'waiting') clearTimeout(this.firstPing.timer)
    this.firstPing = null
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
      socket: {
        // A closing socket would discard the message with a console error: the replay resends what the page sent.
        send: (message) => {
          if (ws.readyState <= WebSocket.OPEN) ws.send(message)
        },
        get bufferedAmount() {
          return ws.bufferedAmount
        },
      },
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
      pair: pairPublishes(),
    }
  }

  private setupHandlers(ws: WebSocket, pieces: WirePieces): void {
    const { downlink, headroom, sender, receiver, pair } = pieces
    this.pieces = pieces
    const take = (frame: DecodedFrame, byteLength: number, at: number): boolean => {
      if (frame.tag === TAG.PIECES_ACK_REQ) return true
      const confirmed = sender.confirmedBytes
      if (frame.tag === TAG.PIECES_ACK) {
        if (!headroom.acknowledged(frame)) return false
        this.headroomChanged(sender)
      } else if (frame.tag === TAG.PONG) {
        sender.heartbeat(frame.took, at)
        this.heartbeatOf(ws)?.resetPong()
        this.owner.onTransportPong(frame.ended)
      } else {
        const paired = pair(frame)
        if (paired) this.owner.onTransportFrame(paired, this, byteLength, ws === this.abandoned?.ws)
      }
      if (sender.confirmedBytes > confirmed) this.heartbeatOf(ws)?.noteConfirmed(at)
      return true
    }
    ws.onmessage = ({ data }: MessageEvent) => {
      const now = performance.now()
      const raw = new Uint8Array(data as ArrayBuffer)
      this.heartbeatOf(ws)?.noteReceived(raw.byteLength, now)
      downlink.delivered(raw.byteLength, now)
      headroom.received()
      if (!receiveMessage(receiver, raw, now, take)) ws.close()
      else if (receiver.heldTag === TAG.RECONCILED && ws !== this.abandoned?.ws)
        this.owner.onTransportReconciledPiece(this)
    }
    ws.onclose = ({ code, reason }: CloseEvent) => {
      headroom.close()
      if (this.ws === ws) this.ws = null
      this.settleDrained()
      // An end the server decided rides the close: its reason is the refusal, read as an answer's is.
      const end = code === WS_CLOSE_REFUSED ? endOfWire(reason) : { otherWireVersion: code === WS_CLOSE_WIRE_VERSION }
      this.owner.onTransportClosed(this, end)
    }
    ws.onerror = () => {}
  }

  /** The heartbeat of `ws`'s wire; none once the page left that socket, which then says nothing of the wire in use. */
  private heartbeatOf(ws: WebSocket): Heartbeat | null {
    return this.ws === ws ? this.heartbeat : null
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
    return this.connecting !== null
  }

  sendFrame({ kind, frame, receivedAt }: OutboundFrame): void {
    assert(this.ws && this.pieces)
    const { receiver } = this.pieces
    if (kind === 'acknowledgement') {
      this.pieces.sender.carry(() => receiver.report(stampWindowHold(frame, receivedAt)))
    } else {
      this.pieces.headroom.send(receiver.report(stampWindowHold(frame, receivedAt)))
    }
    if (this.firstPing && this.firstPing !== 'waiting') this.flushFirstPing()
  }

  /** Its frames wait behind the wire's queues, and for headroom that acknowledgements free. */
  get behindAfter(): number {
    return LOSSY_CONFIRM_SLACK_MS + WIRE_QUEUE_DELAY_MS + this.uplink.roundTrips.pathRtt
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

  /** What waits behind the rest of a frame, which goes whole. */
  dropQueued(ix: number): void {
    this.pieces?.sender.dropWaiting((frame) => {
      const tag = frame[0]
      return tag !== undefined && isSequencedTag(tag) && readU16(frame, 1) === ix
    })
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
    this.probed?.setPingInterval(this.pingInterval)
    // The server sends nothing on a wire before the page's first frame arrived: that frame crossed by now.
    this.pieces.sender.answered(performance.now())
  }

  sendPing(frame: Uint8Array<ArrayBuffer>, now = false): void {
    if (now) this.flushFirstPing()
    if (now || this.firstPing !== 'waiting') return this.sendPingFrame(frame)
    // The first heartbeat can share the controls queued with the first downlink frames.
    this.firstPing = { frame, timer: setTimeout(this.flushFirstPing) }
  }

  private readonly flushFirstPing = (): void => {
    const first = this.firstPing
    if (first === null || first === 'waiting') return
    clearTimeout(first.timer)
    this.firstPing = null
    this.sendPingFrame(first.frame)
  }

  private sendPingFrame(frame: Uint8Array<ArrayBuffer>): void {
    if (this.ws?.readyState !== WebSocket.OPEN) return
    assert(this.pieces)
    this.pieces.sender.send(this.pieces.receiver.report(frame), 0)
  }

  dispose(): void {
    globalThis.removeEventListener?.('pagehide', this.onPageHide)
    this.letGoConnecting()
    // A socket sends what it was handed before its close.
    this.probed?.discard()
    if (this.ws) this.pieces?.sender.flush()
    if (this.ws) this.pieces?.headroom.close()
    const ws = this.ws
    this.ws = null
    if (ws) {
      ws.onopen = ws.onmessage = ws.onerror = ws.onclose = null
      try {
        ws.close(WS_CLOSE_PAGE_LEFT)
      } catch {}
    }
    this.closeAbandonedTransport()
    // What its sender held unsent goes too, though a channel the app keeps still holds this transport.
    this.pieces = null
    this.settleDrained()
  }
}

/** A probed WebSocket and its pieces, the probe's own till the transport adopts them: its first PONG proves the wire,
 *  and from then on, till it ends, what it receives goes to `owner`. */
class PendingProbe implements ProbeWire {
  private phase = 'proving' as 'proving' | 'reporting' | 'adopted' | 'ended'
  private settleProof: (alive: boolean) => void = () => {}
  private readonly proof = new Promise<boolean>((resolve) => (this.settleProof = resolve))
  private readonly timer = setTimeout(() => this.settleProof(false), WS_PROBE_TIMEOUT_MS)
  private readonly ws: WebSocket
  private readonly pieces: WirePieces
  private readonly owner: ProbeOwner
  /** Told once it ends, proven or not. */
  private readonly onEnded: (probe: PendingProbe) => void

  constructor(ws: WebSocket, pieces: WirePieces, owner: ProbeOwner, onEnded: (probe: PendingProbe) => void) {
    this.ws = ws
    this.pieces = pieces
    this.owner = owner
    this.onEnded = onEnded
    ws.onmessage = ({ data }: MessageEvent) => {
      if (!receiveMessage(pieces.receiver, new Uint8Array(data as ArrayBuffer), performance.now(), this.take))
        ws.close()
    }
    ws.onclose = () => {
      const wasReporting = this.phase === 'reporting'
      this.end()
      if (wasReporting) owner.onProbeClosed()
    }
    ws.onerror = () => {}
    ws.onopen = () => pieces.sender.send(encode.ping(), 0)
  }

  /** Whether its PONG came in time; from then on it reports to its owner. If not, it ended. */
  async proven(): Promise<boolean> {
    const alive = await this.proof
    clearTimeout(this.timer)
    // Its socket may have closed between the PONG and this line.
    if (!alive || this.phase === 'ended') {
      this.end()
      return false
    }
    this.phase = 'reporting'
    return true
  }

  /** The transport takes its socket and pieces: the probe owns nothing from here on. */
  handOver(): { ws: WebSocket; pieces: WirePieces } {
    assert(this.phase === 'reporting')
    this.phase = 'adopted'
    return { ws: this.ws, pieces: this.pieces }
  }

  ping(): void {
    try {
      this.pieces.sender.send(withTook(encode.ping(), this.pieces.receiver.took), 0)
    } catch {}
  }

  send(frame: Uint8Array<ArrayBuffer>): void {
    try {
      this.pieces.sender.send(frame, 0)
    } catch {
      // Socket died between the open event and this send — `onProbeClosed` aborts the attempt.
    }
  }

  close(): void {
    if (this.phase !== 'adopted') this.end()
  }

  get uplink(): Uplink {
    return this.pieces.uplink
  }

  setPingInterval(pingInterval: number): void {
    this.pieces.sender.setPingInterval(pingInterval)
  }

  /** The page left for good: its socket says so, and its close ends it. */
  pageLeft(): void {
    try {
      this.ws.close(WS_CLOSE_PAGE_LEFT)
    } catch {}
  }

  /** The transport went: what its sender holds goes out, and it ends telling its owner nothing. */
  discard(): void {
    const { ws } = this
    ws.onopen = ws.onmessage = ws.onerror = ws.onclose = null
    this.pieces.sender.flush()
    this.pageLeft()
    this.end()
  }

  private readonly take = (frame: DecodedFrame, byteLength: number, at: number): boolean => {
    const { headroom, sender } = this.pieces
    if (frame.tag === TAG.PIECES_ACK) return headroom.acknowledged(frame)
    if (frame.tag === TAG.PIECES_ACK_REQ) return true
    if (frame.tag === TAG.PONG) {
      sender.heartbeat(frame.took, at)
      if (this.phase === 'reporting') this.owner.onProbePong()
      else this.settleProof(true)
    } else if (this.phase === 'reporting') {
      const paired = this.pieces.pair(frame)
      if (paired) this.owner.onProbeFrame(paired, byteLength)
    }
    return true
  }

  /** The one way it ends: its socket closed, its proof failed, or the upgrade or the transport let it go. */
  private end(): void {
    if (this.phase === 'ended') return
    this.phase = 'ended'
    clearTimeout(this.timer)
    this.settleProof(false)
    this.pieces.headroom.close()
    this.onEnded(this)
    try {
      this.ws.close()
    } catch {}
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
