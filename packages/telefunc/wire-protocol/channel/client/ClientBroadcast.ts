export { ClientBroadcast }

import type {
  BroadcastBinaryListener,
  BroadcastListener,
  BroadcastListenerByKind,
  BroadcastListeners,
  ChannelData,
  ChannelPublishAck,
  ChannelPublishInfo,
} from '../channel.js'
import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { encode, type ChannelDataFrame, type SequencedEncoder } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import type { BroadcastKind, BroadcastSubscriptions } from '../../wire/payloads.js'
import { PUBLISH_BINARY_HEADER, type WirePublishInfo } from '../../wire/publishInfo.js'
import { HEADER } from '../../wire/header.js'
import { Listeners } from '../../../utils/Listeners.js'
import { broadcastMessages } from '../channelMessages.js'
import { ChannelClosedError, isExpectedChannelFailure } from '../channelErrors.js'
import { classifyTelefuncError } from '../errorClassification.js'
import { isPromise } from '../../../utils/isPromise.js'
import { forListener } from '../listenerAnswer.js'
import { ClientChannel, reportChannelError, type ClientChannelOptions } from './ClientChannel.js'
import type { DomainAnnouncement, DomainRequest } from '../../wire/payloads.js'
import { createDeferred, type Deferred } from '../../../utils/createDeferred.js'

type ClientBroadcastOptions = ClientChannelOptions & {
  lossy?: boolean
  /** The broadcast whose stream it carries, where it is a domain (see `TAG.DOMAIN`). */
  parent?: ClientBroadcast<any>
}

/** A stream's domain: the broadcast that carries it, or null where it stays on its own channel. */
type Domain = ClientBroadcast<any> | null

/** A domain this page asked for, whether the server answered, and the newest publish of a lossy stream waiting for it. */
type AskedDomain = {
  answer: Deferred<Domain>
  answered: boolean
  newestLossy?: { data: Uint8Array; receipt: Promise<ChannelPublishAck> }
}

/** A publish of a lossy stream that may still wait to go, under its seq. */
type NewestPublish = { seq: number; receipt: Promise<ChannelPublishAck>; delta: boolean }

class ClientBroadcast<T = unknown> extends ClientChannel {
  private readonly subscribers: BroadcastListeners<T> = { text: new Listeners(), binary: new Listeners() }
  /** The subscriptions this page asks the server for. */
  private readonly subscribed: BroadcastSubscriptions = { text: false, binary: false }
  /** A newer publish replaces this page's own that no wire has taken yet: see `sendPublish`. */
  private readonly lossy: boolean
  /** This page's newest publish of each lossy stream, while it isn't settled. */
  private readonly newestPublishes = new Map<string, NewestPublish>()
  /** Its streams' domains, as the server announced them. */
  private readonly domains = new Map<string, Domain>()
  /** The domains this page asked for to publish on, till the server's answer is in `domains`. */
  private readonly askedDomains = new Map<string, AskedDomain>()
  /** Where it is a domain: the broadcast whose listeners get its publishes, and whose life it shares. */
  private readonly parent: ClientBroadcast<any> | null

  constructor(options: ClientBroadcastOptions) {
    super(options)
    this.lossy = options.lossy === true
    this.parent = options.parent ?? null
    // Nothing waits on a publish's credit, so the page grants the largest window from the start.
    this.receiveCredit.widenByteWindow()
    this._attach.declareSubscriptions(this.subscribed)
  }

  /** @internal Register a local listener without changing what it asks the server for. */
  _subscribeLocal<K extends BroadcastKind>(kind: K, callback: BroadcastListenerByKind<T>[K]): () => void {
    return this.subscribers[kind].add(callback)
  }

  /** @internal Sends what `reply` resolves to, also while closing: a graceful close waits for it, as for an ack's
   *  answer. A reply it can't send is reported, and `failure` goes instead. */
  _sendReply(reply: Promise<unknown>, failure: unknown): void {
    void this.lifecycle.trackAck(this.sendSettledReply(reply, failure))
  }

  private async sendSettledReply(reply: Promise<unknown>, failure: unknown): Promise<void> {
    const data = await reply
    try {
      this.sendReplyData(data)
    } catch (err) {
      this.lifecycle.handleCallbackError(err)
      this.sendReplyData(failure)
    }
  }

  /** @internal See `ClientChannelTransport.uplinkRate`. */
  get _uplinkRate(): number {
    return this.connection.uplinkRate
  }

  /** @internal A publish of a lossy stream arrived, which the page confirms soon: the server tells by that whether it
   *  keeps up. */
  _confirmSoon(): void {
    this.receiveCredit.confirmSoon()
  }

  /** @internal Declare what it asks the server for; a reconnect carries it in the RECONCILE entry. */
  _setSubscribed(kind: BroadcastKind, on: boolean): void {
    if (on === this.subscribed[kind] || this.stopped) return
    this.subscribed[kind] = on
    if (on) this.connection.sendBroadcastSubscribe(this, kind === 'binary')
    else this.connection.sendBroadcastUnsubscribe(this, kind === 'binary')
  }

  publish(data: ChannelData<T>): Promise<ChannelPublishAck> {
    return reportingUnexpected(this._publishUnreported(data))
  }

  /** @internal A publish that leaves reporting its rejection to the caller. */
  _publishUnreported(
    data: ChannelData<T>,
    lossyStream: string | null = this.lossy ? 'text' : null,
  ): Promise<ChannelPublishAck> {
    if (this.stopped) throw new ChannelClosedError()
    return this.sendPublish(encode.publishAckReq, stringify(data), lossyStream)
  }

  override messages(options?: { signal?: AbortSignal }) {
    return broadcastMessages<ChannelData<T>>(this, options)
  }

  subscribe(callback: BroadcastListener<T>, options: { signal?: AbortSignal } = {}): () => void {
    return this.subscribeWired('text', callback, options)
  }

  publishBinary(data: Uint8Array): Promise<ChannelPublishAck> {
    return reportingUnexpected(this._publishBinaryUnreported(data))
  }

  /** @internal A publish that leaves reporting its rejection to the caller. With `domain`, it goes on that stream's
   *  domain, once the server named it. With `delta`, it needs the publishes of its lossy stream since the newest one
   *  without it, so it takes no waiting publish's place, and none takes its. */
  _publishBinaryUnreported(
    data: Uint8Array,
    lossyStream: string | null = this.lossy ? 'binary' : null,
    domain: string | null = null,
    delta = false,
  ): Promise<ChannelPublishAck> {
    if (this.stopped) throw new ChannelClosedError()
    if (domain === null) return this.sendPublish(encode.publishBinaryAckReq, data, lossyStream, delta)
    const on = this.domainOf(domain)
    if (!isPromise(on)) return (on ?? this)._publishBinaryUnreported(data, lossyStream, null, delta)
    const asked = this.askedDomains.get(domain)!
    // Publishes of a stream go in order, those that wait for its domain first.
    if (lossyStream === null || delta) {
      asked.newestLossy = undefined
      return on.then((domain) => (domain ?? this)._publishBinaryUnreported(data, lossyStream, null, delta))
    }
    // Of a lossy one only the newest waits: an older one gets the newer one's receipt.
    if (asked.newestLossy) {
      asked.newestLossy.data = data
      return asked.newestLossy.receipt
    }
    const newest: NonNullable<AskedDomain['newestLossy']> = {
      data,
      receipt: on.then((domain) => {
        asked.newestLossy = undefined
        return (domain ?? this)._publishBinaryUnreported(newest.data, lossyStream)
      }),
    }
    asked.newestLossy = newest
    return newest.receipt
  }

  /** `stream`'s domain, or the wait for the server's answer to this page's request for it. */
  private domainOf(stream: string): Domain | Promise<Domain> {
    if (this.domains.has(stream)) return this.domains.get(stream)!
    const asked = this.askedDomains.get(stream)
    if (asked) return asked.answer.promise
    const answer = createDeferred<Domain>()
    this.askedDomains.set(stream, { answer, answered: false })
    // Its first reaction, so the publishes that wait for the answer go before any that finds the domain known.
    answer.promise.then(
      (domain) => this.knowDomain(stream, domain),
      () => {},
    )
    const request: DomainRequest = { stream }
    this.connection.sendSequenced(this, 'data', encode.domain, JSON.stringify(request))
    return answer.promise
  }

  /** The server names `stream`'s domain. A replay names it again, which changes nothing. */
  private onPeerDomain(text: string): void {
    const { stream, channelId, lossy } = JSON.parse(text) as DomainAnnouncement
    const asked = this.askedDomains.get(stream)
    if (this.domains.has(stream) || asked?.answered) return
    const domain =
      channelId === null
        ? null
        : new ClientBroadcast({
            channelId,
            lossy,
            parent: this,
            maxFrameBytes: this._maxFrameBytes,
            connection: this.connectionOptions,
          })
    if (asked === undefined) return this.knowDomain(stream, domain)
    asked.answered = true
    asked.answer.resolve(domain)
  }

  private knowDomain(stream: string, domain: Domain): void {
    this.askedDomains.delete(stream)
    if (this.stopped) return domain?._end()
    this.domains.set(stream, domain)
  }

  /** Of `lossyStream`, once its uplink is behind, a publish that still waits to go takes the newer one's data in its
   *  place, and both get its receipt; never where either is `delta`. */
  private sendPublish<D>(
    encodeFrame: SequencedEncoder<D>,
    data: D,
    lossyStream: string | null,
    delta = false,
  ): Promise<ChannelPublishAck> {
    if (lossyStream === null) return this.sendAckReq<ChannelPublishAck, D>(encodeFrame, data)
    const newest = this.newestPublishes.get(lossyStream)
    if (!delta && newest?.delta === false && this.connection.replaceWaiting(this, newest.seq, encodeFrame, data)) {
      return newest.receipt
    }
    let seq = 0
    const receipt = this.sendAckReq<ChannelPublishAck, D>(encodeFrame, data, (queued) => (seq = queued))
    const publish = { seq, receipt, delta }
    this.newestPublishes.set(lossyStream, publish)
    void this.forgetOnceSettled(lossyStream, publish)
    return receipt
  }

  /** A settled publish has nothing left to replace; a newer one may have taken its stream by then. */
  private async forgetOnceSettled(lossyStream: string, publish: NewestPublish): Promise<void> {
    await Promise.allSettled([publish.receipt])
    if (this.newestPublishes.get(lossyStream) === publish) this.newestPublishes.delete(lossyStream)
  }

  subscribeBinary(callback: BroadcastBinaryListener, options: { signal?: AbortSignal } = {}): () => void {
    return this.subscribeWired('binary', callback, options)
  }

  override _end(err?: Error): void {
    super._end(err)
    this.subscribers.text.clear()
    this.subscribers.binary.clear()
    this.newestPublishes.clear()
    for (const domain of this.domains.values()) domain?._end(err)
    this.domains.clear()
    for (const { answer } of this.askedDomains.values()) answer.reject(err ?? new ChannelClosedError())
    this.askedDomains.clear()
  }

  /** The first listener of a kind subscribes the wire, and the last one's removal unsubscribes it. */
  private subscribeWired<K extends BroadcastKind>(
    kind: K,
    callback: BroadcastListenerByKind<T>[K],
    options: { signal?: AbortSignal },
  ): () => void {
    if (options.signal?.aborted) return () => {}
    if (this.subscribers[kind].size === 0) this._setSubscribed(kind, true)
    return this.subscribers[kind].add(callback, {
      ...options,
      onRemove: () => {
        if (this.subscribers[kind].size === 0) this._setSubscribed(kind, false)
      },
    })
  }

  /** The broadcast whose listeners get its publishes: itself, or the one whose domain it is. */
  private get root(): ClientBroadcast<any> {
    return this.parent ?? this
  }

  private sendReplyData(data: unknown): void {
    this.sendCredit.countSent(this.connection.sendSequenced(this, 'data', encode.text, stringify(data)))
  }

  protected override dispatchDataFrame(frame: ChannelDataFrame): void {
    if (frame.tag === TAG.DOMAIN) {
      this.onPeerDomain(frame.text)
      return
    }
    if (frame.tag === TAG.PUBLISH) {
      this.onPeerPublish(frame.text, frame.info, frame.bytes)
      return
    }
    if (frame.tag === TAG.PUBLISH_BINARY) {
      this.onPeerPublishBinary(frame.data as Uint8Array<ArrayBuffer>, frame.info, frame.bytes)
      return
    }
    super.dispatchDataFrame(frame)
  }

  /** A publish counts in bytes, consumed once the listeners ran, so the server sees how far behind the page is. */
  private onPeerPublish(data: string, wireInfo: WirePublishInfo, bytes: number): void {
    this.receiveCredit.onPublishArrived()
    const parsed = parse(data) as ChannelData<T>
    this.deliverPublish(this.root.subscribers.text.list(), () => parsed, wireInfo, bytes)
  }

  private onPeerPublishBinary(data: Uint8Array, wireInfo: WirePublishInfo, bytes: number): void {
    this.receiveCredit.onPublishArrived()
    const list = this.root.subscribers.binary.list()
    this.deliverPublish(list, (i) => forListener(data, list, i, HEADER + PUBLISH_BINARY_HEADER), wireInfo, bytes)
  }

  /** Calls each listener with its value, `valueFor` its index; an `Abort` stops the rest and leaves the publish
   *  unconsumed. A listener's promise isn't awaited. */
  private deliverPublish<V>(
    listeners: readonly ((value: V, info: ChannelPublishInfo) => unknown)[],
    valueFor: (i: number) => V,
    wireInfo: WirePublishInfo,
    bytes: number,
  ): void {
    const { root } = this
    const info = { key: root.key!, seq: wireInfo.seq, timestamp: wireInfo.timestamp }
    for (const [i, cb] of listeners.entries()) {
      try {
        const result = cb(valueFor(i), info)
        if (isPromise(result)) result.catch((err: unknown) => root.lifecycle.handleCallbackError(err))
      } catch (err) {
        if (root.lifecycle.handleCallbackError(err)) return
      }
    }
    this.receiveCredit.onPublishConsumed(bytes)
    if (this.lossy) this._confirmSoon()
  }
}

/** Reports a publish rejection that is a bug; the caller still sees every rejection. */
function reportingUnexpected(publish: Promise<ChannelPublishAck>): Promise<ChannelPublishAck> {
  void reportIfBug(publish)
  return publish
}

async function reportIfBug(publish: Promise<ChannelPublishAck>): Promise<void> {
  try {
    await publish
  } catch (err) {
    if (classifyTelefuncError(err, isExpectedChannelFailure).kind === 'bug') reportChannelError(err)
  }
}
