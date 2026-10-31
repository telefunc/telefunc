export { Liveness }
export type { LivenessOwner }

import type { ChannelRecord, ChannelTable } from './ChannelTable.js'
import type { ClientChannelTransport } from './connection.js'
import type { SendGate } from './SendGate.js'
import { CHANNEL_PING_INTERVAL_MIN_MS, CHANNEL_PING_INTERVAL_MS } from '../../constants.js'
import { encode, type PingEntry, type PongEntry } from '../../wire/frame.js'
import { pingDeadline } from '../../wire/pingDeadline.js'
import { Heartbeat } from './Heartbeat.js'

/** The connection, which drops a wire whose PONG came late and lets go of a channel the server needs nothing more of. */
interface LivenessOwner {
  readonly sessionId: string | null
  onPongTimeout(transport: ClientChannelTransport): void
  releaseChannel(record: ChannelRecord): void
}

type LivenessOptions = {
  owner: LivenessOwner
  channels: ChannelTable
  gate: SendGate
}

/** The heartbeat on the connection's wire: each PING names the closed channels the server attached, and its PONG says
 *  how far the server has what they sent. */
class Liveness {
  private readonly owner: LivenessOwner
  private readonly channels: ChannelTable
  private readonly gate: SendGate
  private pingIntervalMs = CHANNEL_PING_INTERVAL_MS
  /** The heartbeat last installed is the provisional one before a first RECONCILED (see `beatFromReconcile`). */
  private provisionalHeartbeat = false

  constructor({ owner, channels, gate }: LivenessOptions) {
    this.owner = owner
    this.channels = channels
    this.gate = gate
  }

  /** As the last RECONCILED says it. */
  get pingInterval(): number {
    return this.pingIntervalMs
  }

  /** From the RECONCILE on: a RECONCILED comes behind what the server sends as it attaches, however long that takes. */
  beatFromReconcile(transport: ClientChannelTransport): void {
    if (this.owner.sessionId !== null) this.install(transport, this.pingIntervalMs, false)
    else this.install(transport, CHANNEL_PING_INTERVAL_MIN_MS, true)
  }

  /** Idempotent once a RECONCILED set the interval; the next install replaces a provisional one. */
  install(transport: ClientChannelTransport, intervalMs: number, provisional: boolean): void {
    if (transport.heartbeat !== null && !this.provisionalHeartbeat && this.pingIntervalMs === intervalMs) return
    transport.detachHeartbeat()
    if (!provisional) this.pingIntervalMs = intervalMs
    this.provisionalHeartbeat = provisional
    const hb = new Heartbeat({
      intervalMs,
      pongTimeoutMs: pingDeadline(intervalMs),
      send: () => this.beat(transport),
      onDead: () => this.owner.onPongTimeout(transport),
    })
    transport.attachHeartbeat(hb)
    hb.start(!provisional)
  }

  /** How far the server has what the page sent on each channel named, or that it no longer holds it. */
  onPong(ended: PongEntry[]): void {
    for (const { ix, lastSeq } of ended) {
      const record = this.channels.get(ix)
      if (record === undefined || record.state.tag !== 'closed') continue
      if (lastSeq === null) this.owner.releaseChannel(record)
      else this.channels.serverHas(record, record.state, this.channels.sentThrough(ix, lastSeq))
    }
  }

  /** Each open channel acknowledges what arrived since its last WINDOW, then the PING goes. */
  private beat(transport: ClientChannelTransport): void {
    for (const { channel, state } of this.channels.values()) if (state.tag !== 'closed') channel._acknowledge()
    transport.sendPing(this.buildPing())
  }

  /** Names each closed channel the server attached; one a PING named, with nothing to replay or send, is let go. */
  private buildPing(): Uint8Array<ArrayBuffer> {
    const ended: PingEntry[] = []
    let queued: Set<number> | undefined
    for (const record of this.channels.values()) {
      const { ix, state } = record
      if (state.tag !== 'closed' || state.initial) continue
      if (state.reported && record.replay.length === 0) {
        queued ??= this.gate.queuedIxes()
        if (!queued.has(ix)) {
          this.owner.releaseChannel(record)
          continue
        }
      }
      this.channels.reported(state)
      ended.push({ ix, lastSeq: record.lastSeq })
    }
    return encode.ping(ended)
  }
}
