export { getBroadcastAdapter, installBroadcastAdapter, _resetBroadcastAdapterForTesting, DefaultBroadcastAdapter }
export type {
  BroadcastAdapter,
  BroadcastTransport,
  BroadcastUnsubscribe,
  Listening,
  BroadcastPublishResult,
  BroadcastOnMessage,
  BroadcastBinaryOnMessage,
}

import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { Fifo } from '../../utils/Fifo.js'
import { utf8ByteLength } from '../../utils/utf8ByteLength.js'
import { CHANNEL_HOLD_MAX_BYTES } from '../constants.js'
import { ChannelOverflowError } from '../channel/channelErrors.js'
import { Outcome } from '../channel/channel.js'
import { HELD_SEND_OVERHEAD_BYTES } from '../channel/server/channelMemory.js'
import type { WirePublishInfo } from '../wire/publishInfo.js'

/** Transport-level publish result. */
type BroadcastPublishResult = WirePublishInfo & { meta?: Record<string, unknown> }

/** Callback for delivering a broadcast message to a subscriber. */
type BroadcastOnMessage = OnMessage<string>

type BroadcastBinaryOnMessage = OnMessage<Uint8Array>

/** `taken`, on a subscriber with listeners of its own: what delivers a publish to the listeners it has now, for a publish
 *  that waits to be delivered (`queueInMemory`). */
type OnMessage<T> = ((data: T, info: WirePublishInfo) => void) & {
  taken?: () => (data: T, info: WirePublishInfo) => void
}

/** What starting to listen returns: a function that stops listening, where the listener is in effect from the moment it
 *  is returned; else that function as `unsubscribe`, with `opened`, which resolves once the backend delivers to the
 *  listener what any server publishes to its key, and rejects where the backend gives the subscription up. */
type BroadcastUnsubscribe = (() => void) | { unsubscribe: () => void; opened: Promise<void> }

/** A `BroadcastUnsubscribe` in one form: `opened` is undefined where the listener is in effect already, else the one
 *  outcome of its key's subscription of that kind, which every subscriber of the kind to the key shares here. */
type Listening = {
  unsubscribe: () => void
  opened: Outcome | undefined
  /** Aborts where the start the subscriber came for threw, which removed it: nothing of it is to be released any more. */
  removed?: AbortSignal
}

type QueuedPublish = { size: number; depth: number; deliver: () => void }

/** `wireRequest`: the request that opened the wire a subscribing page's messages go out on, where one did. */
type BroadcastAdapter = {
  subscribe(key: string, onMessage: BroadcastOnMessage, wireRequest?: Request): Listening
  publish(key: string, serialized: string): BroadcastPublishResult | Promise<BroadcastPublishResult>
  subscribeBinary(key: string, onMessage: BroadcastBinaryOnMessage, wireRequest?: Request): Listening
  publishBinary(key: string, data: Uint8Array): BroadcastPublishResult | Promise<BroadcastPublishResult>
  /** Where a process runs several objects that can't write to each other's pages, the one a subscriber's page is on. */
  ownerOf?(wireRequest: Request | undefined): string
}

/**
 * Minimal interface for a broadcast transport backend.
 *
 * Implement these 4 methods to get a full BroadcastAdapter via `new DefaultBroadcastAdapter(transport)`.
 * Subscriber multiplexing and lifecycle are handled for you.
 */
type BroadcastTransport = {
  /** Send a text message. Must return the assigned seq and timestamp. */
  send(key: string, payload: string): { seq: number; timestamp: number } | Promise<{ seq: number; timestamp: number }>
  /** Listen for text messages on a key. Called at most once per key. Return an unsubscribe function, or, where the
   *  listener misses publishes until your broker confirms it, `{ unsubscribe, opened }`. */
  listen(
    key: string,
    onMessage: (payload: string, info: { seq: number; timestamp: number }) => void,
  ): BroadcastUnsubscribe
  /** Send a binary message. Must return the assigned seq and timestamp. */
  sendBinary(
    key: string,
    payload: Uint8Array,
  ): { seq: number; timestamp: number } | Promise<{ seq: number; timestamp: number }>
  /** As `listen()`, for binary messages. */
  listenBinary(
    key: string,
    onMessage: (payload: Uint8Array, info: { seq: number; timestamp: number }) => void,
  ): BroadcastUnsubscribe
}

/** How deep in-memory publishes in answer to answers may go. */
const MAX_ANSWER_DEPTH = 10_000

// ---------------------------------------------------------------------------
// Default adapter — subscriber multiplexer + in-memory fallback
// ---------------------------------------------------------------------------

class DefaultBroadcastAdapter implements BroadcastAdapter {
  private readonly transport: BroadcastTransport | null
  private readonly subscriptions = new Map<string, Set<BroadcastOnMessage>>()
  private readonly binarySubscriptions = new Map<string, Set<BroadcastBinaryOnMessage>>()
  /** The transport's one listener of each key, which its subscribers share. */
  private readonly transportUnsubs = new Map<string, Listening>()
  private readonly transportBinaryUnsubs = new Map<string, Listening>()
  /** Per-key seq counter for in-memory mode, while the key has a subscriber. */
  private readonly keySeqs = new Map<string, number>()
  /** The highest seq issued on any key: a key without a counter goes on above it. */
  private highestSeq = 0
  /** In-memory publishes from listeners, waiting for the ones before them, in publish order. */
  private readonly queued = new Fifo<QueuedPublish>()
  /** What `queued` holds: at most `CHANNEL_HOLD_MAX_BYTES`, or one message whatever its size. */
  private queuedBytes = 0
  private delivering = false
  /** How many answers deep the publish being delivered is: 0 for one from outside a listener. */
  private depth = 0

  constructor(transport?: BroadcastTransport) {
    this.transport = transport ?? null
  }

  subscribe(key: string, onMessage: BroadcastOnMessage): Listening {
    return this.register(this.subscriptions, this.transportUnsubs, key, onMessage, (transport, deliver) =>
      transport.listen(key, deliver),
    )
  }

  publish(key: string, serialized: string): BroadcastPublishResult | Promise<BroadcastPublishResult> {
    if (this.transport) return this.transport.send(key, serialized)
    return this.publishInMemory(this.subscriptions, key, serialized)
  }

  subscribeBinary(key: string, onMessage: BroadcastBinaryOnMessage): Listening {
    return this.register(this.binarySubscriptions, this.transportBinaryUnsubs, key, onMessage, (transport, deliver) =>
      transport.listenBinary(key, deliver),
    )
  }

  publishBinary(key: string, data: Uint8Array): BroadcastPublishResult | Promise<BroadcastPublishResult> {
    if (this.transport) return this.transport.sendBinary(key, data)
    return this.publishInMemory(this.binarySubscriptions, key, data)
  }

  /** Adds `onMessage` to `key`'s subscribers of one kind, and has the transport listen on `key` for the first. */
  private register<T>(
    subscriptions: Map<string, Set<OnMessage<T>>>,
    listenings: Map<string, Listening>,
    key: string,
    onMessage: OnMessage<T>,
    listen: (
      transport: BroadcastTransport,
      deliver: (payload: T, info: WirePublishInfo) => void,
    ) => BroadcastUnsubscribe,
  ): Listening {
    const subs = subscriptions.get(key) ?? new Set<OnMessage<T>>()
    subscriptions.set(key, subs)
    subs.add(onMessage)

    // On the set it joined: one a failed start discarded is left empty, so this does nothing to the key's next one.
    const unsubscribe = () => {
      if (!subs.delete(onMessage) || subs.size > 0) return
      subscriptions.delete(key)
      this.releaseUnsub(listenings, key)
      this.releaseSeq(key)
    }

    // Registered before the transport listens, so a message it delivers inside listen() reaches this subscriber; where
    // listen() throws, it is removed again with the key's other subscribers, leaving nothing registered.
    const transport = this.transport
    if (!transport) return { unsubscribe, opened: undefined }
    const listening =
      listenings.get(key) ??
      startListening(listenings, subscriptions, key, () =>
        listen(transport, (payload, info) => {
          this.deliver(subscriptions, key, payload, info)
        }),
      )
    return { unsubscribe, opened: listening.opened, removed: listening.removed }
  }

  // ── In-memory ──

  private publishInMemory<T extends string | Uint8Array>(
    subs: Map<string, Set<OnMessage<T>>>,
    key: string,
    data: T,
  ): BroadcastPublishResult | Promise<BroadcastPublishResult> {
    // A publish from a listener reaches subscribers after the message it answers.
    if (this.delivering) return this.queueInMemory(subs, key, data)
    this.delivering = true
    try {
      return this.deliverInMemory(subs.get(key), data, this.takeInfo(key))
    } finally {
      this.deliverQueued()
      this.delivering = false
    }
  }

  /** Delivers what listeners published in answer, and what they publish in answer to that, in order. */
  private deliverQueued(): void {
    while (this.queued.length > 0) {
      const publish = this.queued.shift()!
      this.queuedBytes -= publish.size
      this.depth = publish.depth
      publish.deliver()
    }
    this.depth = 0
  }

  private queueInMemory<T extends string | Uint8Array>(
    subs: Map<string, Set<OnMessage<T>>>,
    key: string,
    data: T,
  ): Promise<BroadcastPublishResult> {
    if (this.depth === MAX_ANSWER_DEPTH) return Promise.reject(endlessAnswersError())
    const isText = typeof data === 'string'
    const size = (isText ? utf8ByteLength(data) : data.byteLength) + HELD_SEND_OVERHEAD_BYTES
    // One message alone goes through whatever its size.
    if (this.queuedBytes > 0 && this.queuedBytes + size > CHANNEL_HOLD_MAX_BYTES) {
      return Promise.reject(
        new ChannelOverflowError(
          `Publish rejected: publishes waiting to be delivered exceeded ${CHANNEL_HOLD_MAX_BYTES} bytes; publish less from the listener`,
        ),
      )
    }
    this.queuedBytes += size
    // The caller may reuse or transfer its buffer once the publish returns.
    const queuedData = isText ? data : (new Uint8Array(data as Uint8Array) as T)
    // Who gets it and its place in the key's order are the publish's: a subscriber that comes while it waits gets what is
    // published after it, and so does a listener a subscriber adds meanwhile.
    const publishedTo = [...(subs.get(key) ?? [])].map((target) => ({ target, deliver: target.taken?.() ?? target }))
    const info = this.takeInfo(key)
    return new Promise((resolve, reject) => {
      const deliver = () => {
        try {
          resolve(this.deliverInMemory(stillSubscribed(publishedTo, subs.get(key)), queuedData, info))
        } catch (err) {
          reject(err)
        }
      }
      this.queued.push({ size, depth: this.depth + 1, deliver })
    })
  }

  private deliverInMemory<T>(
    targets: Iterable<(data: T, info: WirePublishInfo) => void> | undefined,
    data: T,
    info: WirePublishInfo,
  ): BroadcastPublishResult {
    let delivered = 0
    if (targets) {
      for (const onMessage of targets) {
        delivered++
        onMessage(data, info)
      }
    }
    return { ...info, meta: { delivered, transport: 'in-memory' } }
  }

  /** The `seq` and `timestamp` of a publish to `key` made now. */
  private takeInfo(key: string): WirePublishInfo {
    const seq = (this.keySeqs.get(key) ?? this.highestSeq) + 1
    if (seq > this.highestSeq) this.highestSeq = seq
    if (this.subscriptions.has(key) || this.binarySubscriptions.has(key)) this.keySeqs.set(key, seq)
    return { seq, timestamp: Date.now() }
  }

  // ── Shared ──

  private deliver<T>(
    subs: Map<string, Set<(data: T, info: WirePublishInfo) => void>>,
    key: string,
    data: T,
    info: WirePublishInfo,
  ): void {
    const set = subs.get(key)
    if (!set) return
    for (const onMessage of set) onMessage(data, info)
  }

  private releaseSeq(key: string): void {
    if (!this.subscriptions.has(key) && !this.binarySubscriptions.has(key)) {
      this.keySeqs.delete(key)
    }
  }

  private releaseUnsub(map: Map<string, Listening>, key: string): void {
    const listening = map.get(key)
    if (!listening) return
    map.delete(key)
    listening.unsubscribe()
  }
}

/** The transport's one listener of `key`, in the one form of either `BroadcastUnsubscribe`: its `opened` is observed here
 *  once, for every subscriber of the key, so its first outcome is known to each, read or not. It holds the key's place in
 *  `listenings` while `listen()` runs, as a message delivered inside it may subscribe to the key, which joins this
 *  listener. Where `listen()` throws, the place is given back, and every subscriber of the key in `subscribers` came for
 *  this start: they are removed with it, and the ones that joined get its error. */
function startListening(
  listenings: Map<string, Listening>,
  subscribers: Map<string, Set<unknown>>,
  key: string,
  listen: () => BroadcastUnsubscribe,
): Listening {
  const opened = new Outcome()
  const removed = new AbortController()
  const listening: Listening = { unsubscribe: () => {}, opened, removed: removed.signal }
  listenings.set(key, listening)
  let started: BroadcastUnsubscribe
  try {
    started = listen()
  } catch (err) {
    listenings.delete(key)
    subscribers.get(key)?.clear()
    subscribers.delete(key)
    removed.abort()
    opened.settle(err instanceof Error ? err : new Error(String(err)))
    throw err
  }
  // Nothing can roll the start back now: a subscriber that comes from here on gets no signal to hold.
  listening.removed = undefined
  if (typeof started === 'function') {
    listening.unsubscribe = started
    listening.opened = undefined
    opened.settle(true)
  } else {
    listening.unsubscribe = started.unsubscribe
    void opened.follow(started.opened)
  }
  return listening
}

/** Each of `targets` that `subs` still holds when its turn comes: one that left while the publish waited, or during its
 *  delivery, gets none. */
function* stillSubscribed<T, D>(publishedTo: { target: T; deliver: D }[], subs: Set<T> | undefined): Generator<D> {
  for (const { target, deliver } of publishedTo) if (subs?.has(target)) yield deliver
}

function endlessAnswersError(): ChannelOverflowError {
  return new ChannelOverflowError(
    `Publish rejected: listeners published in answer to each other's answers ${MAX_ANSWER_DEPTH} deep, past Telefunc's limit on nested publishes. A listener that publishes on the key it listens to should skip the messages it published itself.`,
  )
}

// ---------------------------------------------------------------------------
// Global adapter state
// ---------------------------------------------------------------------------

const globalObject = getGlobalObject<{
  adapter: BroadcastAdapter
  installed: boolean
}>('wire-protocol/broadcast/broadcastAdapter.ts', () => ({
  adapter: new DefaultBroadcastAdapter(),
  installed: false,
}))

function getBroadcastAdapter(): BroadcastAdapter {
  return globalObject.adapter
}

/** Per-isolate singleton. The factory runs once: the first caller installs the
 *  adapter, subsequent callers receive the already-installed one. This handles
 *  Vite HMR reloads and bundler quirks that evaluate the user's entry
 *  module more than once in the same isolate. Returns the installed instance so the caller
 *  can keep using the canonical reference. */
function installBroadcastAdapter<T extends BroadcastAdapter>(factory: () => T): T {
  if (!globalObject.installed) {
    globalObject.adapter = factory()
    globalObject.installed = true
  }
  return globalObject.adapter as T
}

/** @internal — test-only escape hatch. */
function _resetBroadcastAdapterForTesting(adapter: BroadcastAdapter): void {
  globalObject.adapter = adapter
  globalObject.installed = true
}
