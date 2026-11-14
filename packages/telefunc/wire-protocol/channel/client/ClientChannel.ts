export { ClientChannel, reportChannelError }
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
} from '../channel.js'
import { LOSSY_WITH_ACK_TEXT, stringifyMessage } from '../channel.js'
import type { TELEFUNC_SHIELDS } from '../../../node/shared/transformer/generateShield/shieldKey.js'
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
import { assert, assertUsage } from '../../../utils/assert.js'
import { Listeners } from '../../../utils/Listeners.js'
import { makeAbortError, makeBugError } from '../../../client/remoteTelefunctionCall/errors.js'
import { ShieldValidationError } from '../../../shared/ShieldValidationError.js'
import { ChannelOverflowError } from '../channelErrors.js'
import { ClientConnection } from './ClientConnection.js'
import { FC_PAGE_YIELD_BYTES, WIRE_MAX_RAW_FRAME_BYTES, type ChannelTransports } from '../../constants.js'
import { ReceiveCredit } from '../../flowControl/ReceiveCredit.js'
import { SelfTime } from '../../flowControl/SelfTime.js'
import { SendCredit } from '../../flowControl/SendCredit.js'
import { normalizeCloseTimeout } from '../normalizeCloseTimeout.js'
import { ChannelLifecycle, type ChannelLifecycleOwner } from '../ChannelLifecycle.js'
import type { Uplink } from '../../flowControl/client/Uplink.js'
import { ChannelAttach } from './ChannelAttach.js'
import type { CreditOwner } from '../../flowControl/credit.js'
import type { Downlink } from '../../flowControl/client/Downlink.js'
import type { MuxChannel, MuxConnection } from './ClientConnection.js'
import { ChannelClosedError } from '../channelErrors.js'
import { channelMessages } from '../channelMessages.js'
import { forListener, listenerAnswer } from '../listenerAnswer.js'
import { isPromise } from '../../../utils/isPromise.js'
import { createDeferred } from '../../../utils/createDeferred.js'

/** What a page's `send(…, { ack: true })` rejects with where the server can't hold it or its answer. */
const ACK_REFUSED_TEXT = 'Send refused: the server holds the most for this channel, or config.channel.maxMemory'

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
  implements ClientChannelType<ClientToServer, ServerToClient>, MuxChannel, ChannelLifecycleOwner
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
  protected stopped = false
  protected readonly lifecycle = new ChannelLifecycle({ owner: this, reportError: reportChannelError })
  /** The ack requests sent and not yet answered, by seq. `resolve` takes `any`: text, binary and publish sends
   *  resolve with different types, each with `parse(text)` of the answer. */
  protected pendingAcks = new Map<number, { resolve: (v: any) => void; reject: (err: Error) => void }>()
  protected selfTime = new SelfTime()
  /** Paces fire-and-forget TEXT, BINARY and PUBLISH sends to what the server grants; see `constants.ts`. */
  protected sendCredit: SendCredit
  protected receiveCredit: ReceiveCredit
  /** @internal Its credits as its connection drives them across wires. */
  readonly _attach: ChannelAttach
  /** This page's newest lossy send, which may still wait to go, and what it settles with. */
  private newestLossy: { seq: number; settled: Promise<any> } | null = null
  /** Sends this page's lossy send that waits for the server to grant more credit; a newer one takes its place. */
  private heldLossy: (() => void) | null = null

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
    // Built before the connection: registering the channel fits its credits to the replay buffers.
    const owner: CreditOwner = {
      // A closed channel's server ends its sends with the close, so the channel grants it no more credit.
      sendByteLimit: (limit, urgency) => {
        if (!this.isClosed) this.connection.sendByteLimit(this, limit, urgency)
      },
      sendMessageLimit: (limit, urgency) => {
        if (!this.isClosed) this.connection.sendMessageLimit(this, limit, urgency)
      },
      sendBdpPing: (probe) => this.connection.sendBdpPing(this, probe),
      backlogBytes: () => this.connection.bufferedAmount(),
      deliveredBefore: (ms) => this.connection.deliveredBefore(ms),
    }
    this.sendCredit = new SendCredit({ owner, selfTime: this.selfTime, yieldBytes: FC_PAGE_YIELD_BYTES })
    this.receiveCredit = new ReceiveCredit({ owner, selfTime: this.selfTime })
    this._attach = new ChannelAttach({ channel: this, sendCredit: this.sendCredit, receiveCredit: this.receiveCredit })
    const config = resolveClientConfig()
    this.connection = ClientConnection.getOrCreate(connection.telefuncUrl, this, {
      transports: connection.transports,
      fetchImpl: (config.fetch ?? globalThis.fetch).bind(globalThis),
      connectionKey: connection.connectionKey,
      headers: connection.headers,
      idleTimeout: connection.idleTimeout,
    })
  }

  get opened(): Promise<void> {
    return this.lifecycle.opened
  }

  get closed(): Promise<void> {
    return this.lifecycle.closed
  }

  messages(options?: { signal?: AbortSignal }) {
    return channelMessages<ChannelData<ServerToClient>>(this, options)
  }

  get isClosed(): boolean {
    return this.stopped
  }

  /** @internal The connection its frames go through. */
  get _connection(): MuxConnection {
    return this.connection
  }

  send(data: ChannelData<ClientToServer>): Promise<void>
  send(data: ChannelData<ClientToServer>, opts: { ack: true }): Promise<ChannelAck<ClientToServer>>
  send(data: ChannelData<ClientToServer>, opts: { ack: false }): Promise<void>
  send(data: ChannelData<ClientToServer>, opts: { lossy: true; ack?: false }): Promise<void>
  send(
    data: ChannelData<ClientToServer>,
    opts?: { ack?: boolean; lossy?: boolean },
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
    opts?: { ack?: boolean; lossy?: boolean },
  ): void | Promise<ChannelAck<ClientToServer>> | Promise<void> {
    if (this.stopped) throw new ChannelClosedError()
    const serialized = stringifyMessage(data, 'send')
    if (opts?.lossy) {
      assertUsage(opts.ack !== true, LOSSY_WITH_ACK_TEXT)
      return this.sendNewest(encode.text, serialized)
    }
    this.flushLossy()
    const needsAck = opts?.ack !== false && (opts?.ack === true || this.ack === true)
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
  sendBinary(data: Uint8Array, opts: { lossy: true; ack?: false }): Promise<void>
  sendBinary(data: Uint8Array, opts?: { ack?: boolean; lossy?: boolean }): Promise<unknown> | Promise<void> {
    const t0 = performance.now()
    try {
      const ret = this._sendBinary(data, opts) ?? Promise.resolve()
      ret.catch(() => {})
      return ret
    } finally {
      this.selfTime.recordSince(t0)
    }
  }

  _sendBinary(data: Uint8Array, opts?: { ack?: boolean; lossy?: boolean }): void | Promise<unknown> | Promise<void> {
    if (this.stopped) throw new ChannelClosedError()
    if (opts?.lossy) {
      assertUsage(opts.ack !== true, LOSSY_WITH_ACK_TEXT)
      return this.sendNewest(encode.binary, data)
    }
    this.flushLossy()
    // Ack-bearing path bypasses credit; see `sendData` for rationale.
    if (opts?.ack === true) {
      return this.sendAckReq<unknown, Uint8Array>(encode.binaryAckReq, data)
    }
    return this.sendCredit.decrement(this.connection.sendSequenced(this, 'data', encode.binary, data))
  }

  listen(callback: ChannelListener<ServerToClient>, options?: { signal?: AbortSignal }): () => void {
    return this.listeners.add(callback, options)
  }

  listenBinary(callback: ChannelBinaryListener, options?: { signal?: AbortSignal }): () => void {
    return this.binaryListeners.add(callback, options)
  }

  onClose(callback: ChannelCloseCallback, options?: { signal?: AbortSignal }): () => void {
    return this.lifecycle.onClose(callback, options)
  }

  onOpen(callback: () => void, options?: { signal?: AbortSignal }): () => void {
    return this.lifecycle.onOpen(callback, options)
  }

  close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult> {
    if (this.lifecycle.closing) return this.lifecycle.closing
    if (this.lifecycle.ended) return Promise.resolve(this.lifecycle.result)
    const timeout = normalizeCloseTimeout(opts?.timeout)
    return this.lifecycle.localCloseRequested({
      timeout,
      peer: 'expected',
      send: () => {
        this.flushLossy()
        this.stopped = true
        this.connection.sendCloseRequest(this, timeout)
      },
    })
  }

  abort(): void
  abort(abortValue: unknown, message?: string): void
  abort(abortValue?: unknown, message?: string): void {
    if (this.stopped) return
    this.stopped = true
    const abortError = createAbortError(abortValue, message)
    this.connection.sendAbort(this, stringify(abortValue))
    this._end(abortError)
  }

  // ── Called by transport connection ──

  _onTransportOpen(batched: boolean, uplink: Uplink, downlink: Downlink): void {
    if (this.stopped) return
    if (batched) this.receiveCredit.useBatchTransportInitial()
    else this.receiveCredit.undoBatchTransportInitial()
    this.receiveCredit.attach(uplink, uplink.roundTrips, downlink)
    this.lifecycle.fireOpen()
  }

  protected onPeerMessage(data: string, bytes: number): void {
    const t0 = performance.now()
    try {
      this.receiveCredit.onReceived(bytes)
      this.callListeners(this.listeners, parse(data) as ChannelData<ServerToClient>, bytes)
    } finally {
      this.selfTime.recordSince(t0)
    }
  }

  protected onPeerAckReqMessage(data: string, seq: number): Promise<void> {
    return this.lifecycle.trackAck(this.answerAckReq(this.listeners, parse(data) as ChannelData<ServerToClient>, seq))
  }

  protected onPeerBinaryAckReqMessage(data: Uint8Array, seq: number): Promise<void> {
    return this.lifecycle.trackAck(this.answerAckReq(this.binaryListeners, data, seq))
  }

  protected onPeerBinaryMessage(data: Uint8Array<ArrayBuffer>, bytes: number): void {
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
    const list = listeners.list()
    for (const [i, cb] of list.entries()) {
      try {
        const result = cb(forListener(value, list, i))
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
   *  BDP accounting happens inside `onPeerMessage` and `onPeerBinaryMessage`. */
  protected dispatchDataFrame(frame: ChannelDataFrame): void {
    switch (frame.tag) {
      case TAG.TEXT:
        this.onPeerMessage(frame.text, frame.bytes)
        if (frame.lossy) this.receiveCredit.confirmSoon()
        return
      case TAG.TEXT_ACK_REQ:
        void this.onPeerAckReqMessage(frame.text, frame.seq)
        return
      case TAG.BINARY:
        this.onPeerBinaryMessage(frame.data as Uint8Array<ArrayBuffer>, frame.bytes)
        if (frame.lossy) this.receiveCredit.confirmSoon()
        return
      case TAG.BINARY_ACK_REQ:
        void this.onPeerBinaryAckReqMessage(frame.data, frame.seq)
        return
      case TAG.ACK_RES:
        this.onPeerAckRes(frame.ackedSeq, frame.text, frame.status)
        return
      case TAG.PUBLISH:
      case TAG.PUBLISH_ACK_REQ:
      case TAG.PUBLISH_BINARY:
      case TAG.PUBLISH_BINARY_ACK_REQ:
      case TAG.PUBLISH_TO:
        // A plain channel drops a publish: `ClientBroadcast` takes PUBLISH and PUBLISH_BINARY before this switch, and
        // the server sends no publish ack request.
        return
    }
  }

  /** @internal — Per-channel ctrl-message switch. Wire-level ctrls (PING/PONG/
   *  FIN/RECONCILED) and channel-termination ctrls (ABORT/ERROR — which involve
   *  connection-side cleanup) are handled by the connection and never reach here. */
  protected dispatchCtrl(frame: ChannelCtrlFrame): void {
    switch (frame.tag) {
      case TAG.CLOSE:
        this.onPeerCloseRequest(frame.timeoutMs)
        return
      case TAG.CLOSE_ACK:
        this.onPeerCloseAck()
        return
      case TAG.WINDOW:
        this.sendCredit.onPeerByteWindow(frame.bytes)
        this.lossyMayGo()
        return
      case TAG.MSG_WINDOW:
        this.sendCredit.onPeerMessageWindow(frame.count)
        this.lossyMayGo()
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

  /** Settle an outstanding ack-bearing send. Mirrors server-side `onPeerAckRes`. */
  protected onPeerAckRes(ackedSeq: number, text: string, status: AckResultStatus): void {
    const pending = this.pendingAcks.get(ackedSeq)
    if (!pending) return
    try {
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
          return
        case ACK_STATUS.OVERFLOW:
          pending.reject(new ChannelOverflowError(ACK_REFUSED_TEXT))
      }
    } catch (err) {
      pending.reject(err instanceof Error ? err : new Error(String(err)))
      throw err
    } finally {
      this.pendingAcks.delete(ackedSeq)
    }
  }

  protected onPeerCloseRequest(timeoutMs: number): void {
    this.lifecycle.peerCloseReceived(normalizeCloseTimeout(timeoutMs), () => {
      this.stopped = true
      this.connection.sendCloseAck(this)
    })
  }

  protected onPeerCloseAck(): void {
    if (this.lifecycle.ended) return
    this.lifecycle.closeAckReceived()
  }

  _onEnded(err?: Error): void {
    this.stopped = true
    // A close the server acknowledged is graceful, whatever takes the channel off the wire before it finishes.
    this._end(this.lifecycle.closeAcked ? undefined : err)
  }

  // ── Private ──

  private async answerAckReq<T>(listeners: Listeners<(data: T) => unknown>, data: T, seq: number): Promise<void> {
    if (listeners.size === 0) {
      this.connection.sendAckRes(this, seq, 'No listener registered for ack request', ACK_STATUS.ERROR)
      return
    }
    let answer: string | null
    try {
      answer = await listenerAnswer(listeners.list(), data)
    } catch (err) {
      if (this.lifecycle.handleCallbackError(err)) return
      this.connection.sendAckRes(this, seq, 'Internal client channel error — see client logs', ACK_STATUS.ERROR)
      return
    }
    if (answer === null) {
      this.connection.sendAckRes(
        this,
        seq,
        'Only one channel listener may return an acknowledgement value',
        ACK_STATUS.ERROR,
      )
      return
    }
    this.connection.sendAckRes(this, seq, answer)
  }

  /** A send a newer one may replace: once the uplink is behind, the newest lossy send that still waits to go, with
   *  nothing sent after it, takes `data` in its place, and both settle as it does; `replaced` gets the bytes that adds.
   *  Else `start` sends it and tells its seq. */
  protected sendLossy<D, R>(
    encodeFrame: SequencedEncoder<D>,
    data: D,
    start: (onQueued: (seq: number) => void) => Promise<R>,
    replaced?: (addedBytes: number) => void,
  ): Promise<R> {
    const newest = this.newestLossy
    if (newest !== null) {
      const added = this.connection.replaceWaiting(this, newest.seq, encodeFrame, data)
      if (added !== null) {
        replaced?.(added)
        return newest.settled
      }
    }
    let seq = 0
    const settled = start((queued) => (seq = queued))
    this.newestLossy = { seq, settled }
    return settled
  }

  /** A send of which only the newest matters. It goes, or takes the place of the newest that still waits for a link
   *  that is behind; while the server grants no more credit, it waits as the newest. */
  private sendNewest<D extends string | Uint8Array>(encodeFrame: SequencedEncoder<D>, data: D): Promise<void> {
    if (this.sendCredit.isOutOfCredit()) {
      // Too large, it throws here, and the one held stays.
      this.connection.assertFits(this, encodeFrame, data)
      // The caller may write into its bytes once `sendBinary` returned.
      const copy = (typeof data === 'string' ? data : new Uint8Array(data)) as D
      this.heldLossy = () => void this.handLossy(encodeFrame, copy)
      return Promise.resolve()
    }
    this.heldLossy = null
    return this.handLossy(encodeFrame, data)
  }

  /** Counts a lossy send against the server's credit, and nothing waits on that: a replacement counts the bytes it
   *  adds. */
  private handLossy<D>(encodeFrame: SequencedEncoder<D>, data: D): Promise<void> {
    return this.sendLossy(
      encodeFrame,
      data,
      (onQueued) => {
        this.sendCredit.count(this.connection.sendSequenced(this, 'data', encodeFrame, data, onQueued))
        return Promise.resolve()
      },
      (added) => this.sendCredit.refund(-added, 0),
    )
  }

  /** The lossy send held for credit goes now, ahead of what the page sends next. */
  private flushLossy(): void {
    const send = this.heldLossy
    if (send === null) return
    this.heldLossy = null
    send()
  }

  /** The server granted more: the lossy send held for credit goes, where it no longer has to wait. */
  private lossyMayGo(): void {
    if (this.heldLossy !== null && !this.sendCredit.isOutOfCredit()) this.flushLossy()
  }

  /** An ack-bearing send: what refuses it throws here, as it does for a send without an ack. */
  protected sendAckReq<T, D>(encodeFrame: SequencedEncoder<D>, data: D, onQueued?: (seq: number) => void): Promise<T> {
    const ack = createDeferred<T>()
    this.connection.sendSequenced(this, 'ack', encodeFrame, data, (seq) => {
      this.pendingAcks.set(seq, ack)
      onQueued?.(seq)
    })
    return this.lifecycle.trackAck(ack.promise)
  }

  /** @internal */
  _end(err?: Error): void {
    if (this.lifecycle.ended) return
    this.lifecycle.forcedTermination(err, () => {
      this.heldLossy = null
      this.listeners.clear()
      this.binaryListeners.clear()
      this.sendCredit.shutdown(err)
      this.receiveCredit.detach()
      const ackErr = err ?? new ChannelClosedError()
      for (const { reject } of this.pendingAcks.values()) reject(ackErr)
      this.pendingAcks.clear()
      this.connection.unregister(this)
    })
  }

  /** @internal */
  _abortFromCallback(err: AbortError): void {
    this.abort(err.abortValue, err.message)
  }
}

function reportChannelError(err: unknown): void {
  console.error('[telefunc:channel-error]', err instanceof Error ? err : new Error(String(err)))
}
