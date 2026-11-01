export { ClientChannel, ClientBroadcast }
export type { ClientChannelOptions, ConnectionOptions }

import type {
  ChannelAck,
  ClientChannel as ClientChannelType,
  ChannelCloseCallback,
  ChannelCloseOptions,
  ChannelCloseResult,
  ChannelData,
  ChannelListener,
  ChannelBinaryListener,
  ChannelPublishAck,
  BroadcastBinaryListener,
  BroadcastListener,
} from '../channel.js'
import type { TELEFUNC_SHIELDS } from '../../../node/shared/transformer/generateShield/shieldKey.js'
import { makePublishInfo } from '../channel.js'
import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { resolveClientConfig } from '../../../client/clientConfig.js'
import { createAbortError, type AbortError } from '../../../shared/Abort.js'
import {
  ACK_STATUS,
  encode,
  type AckResultStatus,
  type ChannelCtrlFrame,
  type ChannelDataFrame,
  type ChannelFrame,
  type SequencedEncoder,
} from '../../wire/frame.js'
import { TAG, countsCredit, isChannelCtrlTag } from '../../wire/tags.js'
import type { AttachState } from '../../wire/payloads.js'
import type { WirePublishInfo } from '../../wire/publishInfo.js'
import { assert } from '../../../utils/assert.js'
import { Listeners } from '../../../utils/Listeners.js'
import { makeAbortError, makeBugError } from '../../../client/remoteTelefunctionCall/errors.js'
import { ShieldValidationError } from '../../../shared/ShieldValidationError.js'
import { ClientConnection } from './connection.js'
import { getSessionUrl } from './getSessionUrl.js'
import { FC_PAGE_YIELD_BYTES, WIRE_MAX_RAW_FRAME_BYTES, type ChannelTransports } from '../../constants.js'
import type { CreditOwner, Urgency } from '../../flowControl/credit.js'
import { ReceiveCredit } from '../../flowControl/ReceiveCredit.js'
import { SelfTime } from '../../flowControl/SelfTime.js'
import { SendCredit } from '../../flowControl/SendCredit.js'
import { normalizeCloseTimeout } from '../normalizeCloseTimeout.js'
import { ChannelLifecycle, type ChannelLifecycleOwner } from '../ChannelLifecycle.js'
import type { Uplink } from '../../flowControl/client/Uplink.js'
import type { Downlink } from '../../flowControl/client/Downlink.js'
import type { MuxChannel, MuxConnection } from './connection.js'
import { ChannelClosedError } from '../channelErrors.js'
import { isPromise } from '../../../utils/isPromise.js'
import { hasProp } from '../../../utils/hasProp.js'

const CLIENT_BROADCAST_BRAND = Symbol.for('telefunc.ClientBroadcast')

/** The connection a channel rides (see `ClientConnection.getOrCreate`). */
type ConnectionOptions = {
  transports: ChannelTransports
  connectionKey?: string
  headers?: Record<string, string>
  telefuncUrl: string
  idleTimeout?: number
}

type ClientChannelOptions = {
  channelId: string
  ack?: boolean
  key?: string
  maxFrameBytes?: number
  connection: ConnectionOptions
}

class ClientChannel<ClientToServer = unknown, ServerToClient = unknown>
  implements ClientChannelType<ClientToServer, ServerToClient>, MuxChannel, CreditOwner, ChannelLifecycleOwner
{
  /** @see ChannelShield in ../channel.ts — the server checks with it: `data` what this page sends, `ack` its answers
   *  to the server's ack requests. */
  declare readonly [TELEFUNC_SHIELDS]: {
    data: ChannelData<ClientToServer>
    ack: ChannelAck<ServerToClient>
  }
  readonly id: string
  readonly ack: boolean
  readonly key: string | undefined
  readonly _maxFrameBytes: number
  protected connection: MuxConnection
  private readonly listeners = new Listeners<ChannelListener<ServerToClient>>()
  private readonly binaryListeners = new Listeners<ChannelBinaryListener>()
  protected closed = false
  protected readonly lifecycle = new ChannelLifecycle({ owner: this })
  /** The ack requests sent and not yet answered, by seq. `resolve` takes `any`: text, binary and publish sends
   *  resolve with different types, each with `parse(text)` of the answer. */
  protected pendingAcks = new Map<number, { resolve: (v: any) => void; reject: (err: Error) => void }>()
  protected selfTime = new SelfTime()
  /** Paces fire-and-forget TEXT, BINARY and PUBLISH sends to what the server grants; see `constants.ts`. */
  protected sendCredit: SendCredit
  protected receiveCredit: ReceiveCredit

  constructor({
    channelId,
    ack = false,
    key,
    maxFrameBytes = WIRE_MAX_RAW_FRAME_BYTES,
    connection,
  }: ClientChannelOptions) {
    this.id = channelId
    this.ack = ack
    this.key = key
    this._maxFrameBytes = maxFrameBytes
    this.sendCredit = new SendCredit({ owner: this, selfTime: this.selfTime, yieldBytes: FC_PAGE_YIELD_BYTES })
    this.receiveCredit = new ReceiveCredit({ owner: this, selfTime: this.selfTime })
    const config = resolveClientConfig()
    this.connection = ClientConnection.getOrCreate(getSessionUrl(connection.telefuncUrl), this, {
      transports: connection.transports,
      fetchImpl: (config.fetch ?? globalThis.fetch).bind(globalThis),
      connectionKey: connection.connectionKey,
      headers: connection.headers,
      idleTimeout: connection.idleTimeout,
    })
  }

  get isClosed(): boolean {
    return this.closed
  }

  send(data: ChannelData<ClientToServer>): Promise<void>
  send(data: ChannelData<ClientToServer>, opts: { ack: true }): Promise<ChannelAck<ClientToServer>>
  send(data: ChannelData<ClientToServer>, opts: { ack: false }): Promise<void>
  send(
    data: ChannelData<ClientToServer>,
    opts?: { ack?: boolean },
  ): Promise<ChannelAck<ClientToServer>> | Promise<void> {
    const t0 = performance.now()
    try {
      const ret = this.sendData(data, opts) ?? Promise.resolve()
      ret.catch(() => {})
      return ret
    } finally {
      this.selfTime.recordSince(t0)
    }
  }

  private sendData(
    data: ChannelData<ClientToServer>,
    opts?: { ack?: boolean },
  ): void | Promise<ChannelAck<ClientToServer>> | Promise<void> {
    if (this.closed) throw new ChannelClosedError()
    const needsAck = opts?.ack !== false && (opts?.ack === true || this.ack === true)
    const serialized = stringify(data)
    // Ack-bearing sends bypass credit accounting — the caller's `await` on the ack
    // Promise already serializes the next send, so credit would add nothing.
    if (needsAck) {
      return this.sendAckReq<ChannelAck<ClientToServer>, string>(encode.textAckReq, serialized)
    }
    return this.sendCredit.decrement(this.connection.sendSequenced(this, 'data', encode.text, serialized))
  }

  sendBinary(data: Uint8Array): Promise<void>
  sendBinary(data: Uint8Array, opts: { ack: true }): Promise<unknown>
  sendBinary(data: Uint8Array, opts: { ack: false }): Promise<void>
  sendBinary(data: Uint8Array, opts?: { ack?: boolean }): Promise<unknown> | Promise<void> {
    const t0 = performance.now()
    try {
      const ret = this._sendBinary(data, opts) ?? Promise.resolve()
      ret.catch(() => {})
      return ret
    } finally {
      this.selfTime.recordSince(t0)
    }
  }

  _sendBinary(data: Uint8Array, opts?: { ack?: boolean }): void | Promise<unknown> | Promise<void> {
    if (this.closed) throw new ChannelClosedError()
    // Ack-bearing path bypasses credit; see `sendData` for rationale.
    if (opts?.ack === true) {
      return this.sendAckReq<unknown, Uint8Array>(encode.binaryAckReq, data)
    }
    return this.sendCredit.decrement(this.connection.sendSequenced(this, 'data', encode.binary, data))
  }

  listen(callback: ChannelListener<ServerToClient>): () => void {
    return this.listeners.add(callback)
  }

  listenBinary(callback: ChannelBinaryListener): () => void {
    return this.binaryListeners.add(callback)
  }

  onClose(callback: ChannelCloseCallback): void {
    this.lifecycle.onClose(callback)
  }

  onOpen(callback: () => void): void {
    this.lifecycle.onOpen(callback)
  }

  close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult> {
    if (this.lifecycle.closing) return this.lifecycle.closing
    if (this.lifecycle.ended) return Promise.resolve(this.lifecycle.result)
    const timeout = normalizeCloseTimeout(opts?.timeout)
    this.lifecycle.startClose(timeout, true)
    this.closed = true
    this.connection.sendCloseRequest(this, timeout)
    return this.lifecycle.runClose()
  }

  /** @internal How long a server that is away is waited for. */
  _reconnectWindow(): number {
    return this.connection.reconnectWindow()
  }

  abort(): void
  abort(abortValue: unknown, message?: string): void
  abort(abortValue?: unknown, message?: string): void {
    if (this.closed) return
    this.closed = true
    const abortError = createAbortError(abortValue, message)
    this.connection.sendAbort(this, stringify(abortValue))
    this._end(abortError)
  }

  // ── Called by its credit (see `CreditOwner`) ──

  /** @internal A closed channel's server ends its sends with the close, so the channel grants it no more credit. */
  _sendByteLimit(limit: number, urgency: Urgency): void {
    if (!this.closed) this.connection.sendByteLimit(this, limit, urgency)
  }

  /** @internal */
  _sendMessageLimit(limit: number, urgency: Urgency): void {
    if (!this.closed) this.connection.sendMessageLimit(this, limit, urgency)
  }

  /** @internal */
  _sendBdpPing(probe: number): void {
    this.connection.sendBdpPing(this, probe)
  }

  /** @internal */
  _backlogBytes(): number | undefined {
    return this.connection.bufferedAmount()
  }

  /** @internal */
  _deliveredBefore(ms: number): number {
    return this.connection.deliveredBefore(ms)
  }

  // ── Called by transport connection ──

  /** @internal Probes a path it hasn't measured. */
  _attachState(uplink: Uplink): AttachState {
    const probe = this.receiveCredit.probeAttach(uplink.roundTrips)
    return probe === undefined ? {} : { probe }
  }

  /** @internal */
  _onProbeAnswered(probe: number): void {
    this.receiveCredit.onProbeAnswered(probe)
  }

  /** @internal */
  _fitReplays(window: number, peerWindow: number): void {
    this.receiveCredit.fitReplay(window)
    this.sendCredit.fitReplay(peerWindow)
  }

  /** @internal At each heartbeat: see `ReceiveCredit.acknowledge`. */
  _acknowledge(): void {
    this.receiveCredit.acknowledge()
  }

  /** @internal See `ReceiveCredit.answered`. */
  _answered(): void {
    this.receiveCredit.answered()
  }

  /** @internal */
  _onTransportBatched(): void {
    if (!this.closed) this.receiveCredit.useBatchTransportInitial()
  }

  /** @internal */
  _onUpgradeJoined(): void {
    if (this.closed) return
    this.receiveCredit.undoBatchTransportInitial()
    this.receiveCredit.restartRound()
  }

  _onTransportOpen(batched: boolean, uplink: Uplink, downlink: Downlink): void {
    if (this.closed) return
    if (batched) this.receiveCredit.useBatchTransportInitial()
    else this.receiveCredit.undoBatchTransportInitial()
    this.receiveCredit.attach(uplink, uplink.roundTrips, downlink)
    this.lifecycle.fireOpen()
  }

  _onPeerMessage(data: string, bytes: number): void {
    const t0 = performance.now()
    try {
      this.receiveCredit.onReceived(bytes)
      this.callListeners(this.listeners, parse(data) as ChannelData<ServerToClient>, bytes)
    } finally {
      this.selfTime.recordSince(t0)
    }
  }

  _onPeerAckReqMessage(data: string, seq: number): Promise<void> {
    return this.lifecycle.trackAck(this.answerAckReq(this.listeners, parse(data) as ChannelData<ServerToClient>, seq))
  }

  _onPeerBinaryAckReqMessage(data: Uint8Array, seq: number): Promise<void> {
    return this.lifecycle.trackAck(this.answerAckReq(this.binaryListeners, data, seq))
  }

  _onPeerBinaryMessage(data: Uint8Array<ArrayBuffer>, bytes: number): void {
    const t0 = performance.now()
    try {
      this.receiveCredit.onReceived(bytes)
      this.callListeners(this.binaryListeners, data, bytes)
    } finally {
      this.selfTime.recordSince(t0)
    }
  }

  /** Calls each of `listeners` with `value`; its `bytes` are consumed once the promises they return settle. A listener
   *  that throws `Abort` stops the rest. */
  private callListeners<T>(listeners: Listeners<(value: T) => unknown>, value: T, bytes: number): void {
    const pending: Promise<unknown>[] = []
    for (const cb of listeners.list()) {
      try {
        const result = cb(value)
        if (isPromise(result)) {
          pending.push(result.catch((err: unknown) => this.lifecycle.handleCallbackError(err)))
        }
      } catch (err) {
        if (this.lifecycle.handleCallbackError(err)) return
      }
    }
    if (pending.length > 0) {
      Promise.all(pending).finally(() => this.receiveCredit.onConsumed(bytes))
    } else {
      this.receiveCredit.onConsumed(bytes)
    }
  }

  /** @internal — Entry point for every per-channel wire frame. The connection routes
   *  here for any frame carrying an index; this method then splits ctrl vs data. */
  _dispatchFrame(frame: ChannelFrame): void {
    if (isChannelCtrlTag(frame.tag)) {
      this.dispatchCtrl(frame as ChannelCtrlFrame)
      return
    }
    const data = frame as ChannelDataFrame
    if (!countsCredit(data.tag)) this.receiveCredit.onReceivedUncounted(data.bytes)
    this.dispatchDataFrame(data)
  }

  /** @internal — Tag-keyed data-frame switch. `ClientBroadcast` overrides to add the
   *  PUBLISH variants and falls back to `super` for the common cases. Receive-time
   *  BDP accounting happens inside `_onPeerMessage` and `_onPeerBinaryMessage`. */
  protected dispatchDataFrame(frame: ChannelDataFrame): void {
    switch (frame.tag) {
      case TAG.TEXT:
        this._onPeerMessage(frame.text, frame.bytes)
        return
      case TAG.TEXT_ACK_REQ:
        void this._onPeerAckReqMessage(frame.text, frame.seq)
        return
      case TAG.BINARY:
        this._onPeerBinaryMessage(frame.data as Uint8Array<ArrayBuffer>, frame.bytes)
        return
      case TAG.BINARY_ACK_REQ:
        void this._onPeerBinaryAckReqMessage(frame.data, frame.seq)
        return
      case TAG.ACK_RES:
        this.onPeerAckRes(frame.ackedSeq, frame.text, frame.status)
        return
      case TAG.PUBLISH:
      case TAG.PUBLISH_ACK_REQ:
      case TAG.PUBLISH_BINARY:
      case TAG.PUBLISH_BINARY_ACK_REQ:
        // PUBLISH variants land here only on `ClientBroadcast`; non-broadcast channels drop them.
        return
    }
  }

  /** @internal — Per-channel ctrl-message switch. Connection-level ctrls (PING/PONG/
   *  FIN/RECONCILED) and channel-termination ctrls (ABORT/ERROR — which involve
   *  connection-side cleanup) are handled by the connection and never reach here. */
  protected dispatchCtrl(frame: ChannelCtrlFrame): void {
    switch (frame.tag) {
      case TAG.CLOSE:
        this._onPeerCloseRequest(frame.timeoutMs)
        return
      case TAG.CLOSE_ACK:
        this._onPeerCloseAck()
        return
      case TAG.WINDOW:
        this.sendCredit.onPeerByteWindow(frame.bytes)
        return
      case TAG.MSG_WINDOW:
        this.sendCredit.onPeerMessageWindow(frame.count)
        return
      case TAG.BDP_PING:
        this.connection.sendBdpPingAck(this, {
          probe: frame.probe,
          backlog: this.sendCredit.onPing(),
          pathRtt: this.receiveCredit.pathRtt(),
        })
        return
      case TAG.BDP_PING_ACK:
        this.receiveCredit.onPingAck(frame)
        return
      // BROADCAST_SUB/UNSUB are server-side only; ABORT/ERROR handled by connection.
    }
  }

  /** Settle an outstanding ack-bearing send. Mirrors server-side `_onPeerAckRes`. */
  protected onPeerAckRes(ackedSeq: number, text: string, status: AckResultStatus): void {
    const pending = this.pendingAcks.get(ackedSeq)
    if (!pending) return
    this.pendingAcks.delete(ackedSeq)
    switch (status) {
      case ACK_STATUS.OK:
        pending.resolve(parse(text))
        return
      case ACK_STATUS.ABORT:
        pending.reject(makeAbortError(parse(text)))
        return
      case ACK_STATUS.ERROR:
        pending.reject(makeBugError(text || undefined))
        return
      case ACK_STATUS.SHIELD_ERROR:
        pending.reject(new ShieldValidationError(text))
    }
  }

  _onPeerCloseRequest(timeoutMs: number): void {
    this.lifecycle.shortenDeadline(normalizeCloseTimeout(timeoutMs))
    this.connection.sendCloseAck(this)
    if (this.closed) return
    this.closed = true
    void this.lifecycle.finish()
  }

  _onPeerCloseAck(): void {
    if (this.lifecycle.ended) return
    this.lifecycle.onCloseAck()
  }

  _onEnded(err?: Error): void {
    this.closed = true
    // A close the server acknowledged is graceful, whatever takes the channel off the wire before it finishes.
    this._end(this.lifecycle.closeAcked ? undefined : err)
  }

  // ── Private ──

  private async answerAckReq<T>(listeners: Listeners<(data: T) => unknown>, data: T, seq: number): Promise<void> {
    if (listeners.size === 0) {
      this.connection.sendAckRes(this, seq, 'No listener registered for ack request', ACK_STATUS.ERROR)
      return
    }
    let answer: string
    try {
      let lastResult: unknown
      for (const cb of listeners.list()) lastResult = await cb(data)
      answer = stringify(lastResult)
    } catch (err) {
      if (this.lifecycle.handleCallbackError(err)) return
      this.connection.sendAckRes(this, seq, 'Internal client channel error — see client logs', ACK_STATUS.ERROR)
      return
    }
    this.connection.sendAckRes(this, seq, answer)
  }

  /** An ack-bearing send: what refuses it throws here, as it does for a send without an ack. */
  protected sendAckReq<T, D>(encodeFrame: SequencedEncoder<D>, data: D): Promise<T> {
    let pending!: { resolve: (v: T) => void; reject: (err: Error) => void }
    const ack = new Promise<T>((resolve, reject) => (pending = { resolve, reject }))
    this.connection.sendSequenced(this, 'ack', encodeFrame, data, (seq) => this.pendingAcks.set(seq, pending))
    return this.lifecycle.trackAck(ack)
  }

  /** @internal */
  _end(err?: Error): void {
    if (this.lifecycle.ended) return
    this.lifecycle.end(err)
    this.sendCredit.shutdown()
    this.receiveCredit.detach()
    const ackErr = err ?? new ChannelClosedError()
    for (const { reject } of this.pendingAcks.values()) reject(ackErr)
    this.pendingAcks.clear()
    this.connection.unregister(this)
    this.lifecycle.fireClose(err)
  }

  /** @internal */
  _abortFromCallback(err: AbortError): void {
    this.abort(err.abortValue, err.message)
  }

  /** @internal */
  _reportCallbackError(err: unknown): void {
    reportChannelError(err)
  }
}

class ClientBroadcast<T = unknown> extends ClientChannel {
  readonly [CLIENT_BROADCAST_BRAND] = true
  private readonly broadcastListeners = new Listeners<BroadcastListener<T>>()
  private readonly broadcastBinaryListeners = new Listeners<BroadcastBinaryListener>()
  /** The subscriptions this page asks the server for. */
  private readonly subscribed = { text: false, binary: false }

  constructor(options: ClientChannelOptions) {
    super(options)
    // Nothing waits on a publish's credit: the window only bounds how far behind the server lets the page fall.
    this.receiveCredit.widenByteWindow()
  }

  static isClientBroadcast(value: unknown): value is ClientBroadcast {
    return hasProp(value, CLIENT_BROADCAST_BRAND)
  }

  publish(data: ChannelData<T>): Promise<ChannelPublishAck> {
    if (this.closed) throw new ChannelClosedError()
    const serialized = stringify(data)
    const ret = this.sendAckReq<ChannelPublishAck, string>(encode.publishAckReq, serialized)
    ret.catch(reportChannelError)
    return ret
  }

  subscribe(callback: BroadcastListener<T>): () => void {
    if (this.broadcastListeners.size === 0) this.setSubscribed('text', true)
    const remove = this.broadcastListeners.add(callback)
    return () => {
      remove()
      if (this.broadcastListeners.size === 0) this.setSubscribed('text', false)
    }
  }

  publishBinary(data: Uint8Array): Promise<ChannelPublishAck> {
    if (this.closed) throw new ChannelClosedError()
    const ret = this.sendAckReq<ChannelPublishAck, Uint8Array>(encode.publishBinaryAckReq, data)
    ret.catch(reportChannelError)
    return ret
  }

  subscribeBinary(callback: BroadcastBinaryListener): () => void {
    if (this.broadcastBinaryListeners.size === 0) this.setSubscribed('binary', true)
    const remove = this.broadcastBinaryListeners.add(callback)
    return () => {
      remove()
      if (this.broadcastBinaryListeners.size === 0) this.setSubscribed('binary', false)
    }
  }

  private setSubscribed(kind: 'text' | 'binary', on: boolean): void {
    if (on === this.subscribed[kind] || this.closed) return
    this.subscribed[kind] = on
    if (on) this.connection.sendBroadcastSubscribe(this, kind === 'binary')
    else this.connection.sendBroadcastUnsubscribe(this, kind === 'binary')
  }

  /** @internal Declares the subscriptions on every (re)attach: a SUB or UNSUB written to a dead wire isn't replayed. */
  override _attachState(): AttachState {
    return { broadcast: this.subscribed }
  }

  protected override dispatchDataFrame(frame: ChannelDataFrame): void {
    if (frame.tag === TAG.PUBLISH) {
      this._onPeerPublish(frame.text, frame.info, frame.bytes)
      return
    }
    if (frame.tag === TAG.PUBLISH_BINARY) {
      this._onPeerPublishBinary(frame.data as Uint8Array<ArrayBuffer>, frame.info, frame.bytes)
      return
    }
    super.dispatchDataFrame(frame)
  }

  /** A publish counts in bytes, consumed once the listeners ran, so the server sees how far behind the page is. */
  _onPeerPublish(data: string, wireInfo: WirePublishInfo, bytes: number): void {
    this.receiveCredit.onPublishArrived()
    const parsed = parse(data) as ChannelData<T>
    const info = makePublishInfo(this.key!, wireInfo.seq, wireInfo.timestamp)
    for (const cb of this.broadcastListeners.list()) {
      try {
        const result = cb(parsed, info)
        if (isPromise(result)) result.catch((err: unknown) => this.lifecycle.handleCallbackError(err))
      } catch (err) {
        if (this.lifecycle.handleCallbackError(err)) return
      }
    }
    this.receiveCredit.onPublishConsumed(bytes)
  }

  _onPeerPublishBinary(data: Uint8Array, wireInfo: WirePublishInfo, bytes: number): void {
    this.receiveCredit.onPublishArrived()
    const info = makePublishInfo(this.key!, wireInfo.seq, wireInfo.timestamp)
    for (const cb of this.broadcastBinaryListeners.list()) {
      try {
        // Bytes of its own: a listener may write into them or transfer their buffer.
        const result = cb(new Uint8Array(data), info)
        if (isPromise(result)) result.catch((err: unknown) => this.lifecycle.handleCallbackError(err))
      } catch (err) {
        if (this.lifecycle.handleCallbackError(err)) return
      }
    }
    this.receiveCredit.onPublishConsumed(bytes)
  }
}

function reportChannelError(err: unknown): void {
  console.error('[telefunc:channel-error]', err instanceof Error ? err : new Error(String(err)))
}
