// What a page is given when its broadcast's subscription doesn't take effect on the server: the REAL ChannelMux and
// ServerBroadcast end the broadcast, and the ERROR frame they send its page reaches the REAL ClientBroadcast, as sent, over
// an SSE wire this test serves.

import { afterEach, expect, test, vi } from 'vitest'
import { createHook } from 'node:async_hooks'

import { ClientBroadcast } from './client/ClientBroadcast.js'
import { getChannelMux } from './server/ChannelMux.js'
import { ServerBroadcast } from './server/ServerBroadcast.js'
import { IndexedPeer, type PeerSender } from './server/IndexedPeer.js'
import { ChannelClosedError } from './channelErrors.js'
import { configureBroadcastTransport, disposeBackend } from '../backend/install.js'
import type { BroadcastUnsubscribe } from '../backend/broadcast/TransportBroadcastDriver.js'
import { decode, encode, type DecodedFrame } from '../wire/frame.js'
import { TAG } from '../wire/tags.js'
import { parseSseRequestMetadata } from '../wire/envelope.js'
import { BodyReader } from '../wire/BodyReader.js'
import { queueMemory } from './server/channelMemory.js'
import { uint8ArrayToBase64url } from '../wire/base64url.js'
import { CHANNEL_TRANSPORT, SSE_METADATA_MAX_BYTES, WIRE_MAX_RAW_FRAME_BYTES } from '../constants.js'
import { createDeferred, type Deferred } from '../../utils/createDeferred.js'
import { collectGarbage } from '../test/collectGarbage.js'
import { ATTACHABLE_NOW } from './server/ServerChannel.js'

afterEach(() => {
  vi.unstubAllGlobals()
  vi.useRealTimers()
  disposeBackend()
})

const ENDINGS: Record<string, { listen: () => BroadcastUnsubscribe; pastLimit: boolean }> = {
  'its backend refuses it': {
    listen: () => ({ unsubscribe() {}, opened: Promise.reject(new Error('NOPERM: no permission for this channel')) }),
    pastLimit: false,
  },
  'its backend neither confirms nor refuses it within `connectTtl` of a call that carries it': {
    listen: () => ({ unsubscribe() {}, opened: new Promise<void>(() => {}) }),
    pastLimit: true,
  },
}

for (const [ending, { listen, pastLimit }] of Object.entries(ENDINGS)) {
  test(`where ${ending}, the page's \`opened\` and \`onClose()\` get a ChannelClosedError, not the server's error`, async () => {
    const page = await subscribedPage()
    page.receive(await errorSentToPage(page.index, listen, pastLimit))
    const given = await page.given()
    expect(given.opened).toBeInstanceOf(ChannelClosedError)
    expect(given.onClose).toBeInstanceOf(ChannelClosedError)
  })
}

// A page's subscription that took effect stays one that did when `config.broadcast.transport` is replaced under it.
for (const kind of ['text', 'binary'] as const) {
  const ENDED = 'its subscription was in effect on the server until its backend ended it'
  const NOT_IN_EFFECT = 'its subscription did not take effect on the server'
  for (const [before, outcome] of [
    ['took effect and heard a publish', ENDED],
    ['never took effect', NOT_IN_EFFECT],
  ] as const) {
    test(`${kind}: a page whose subscription ${before} gets "${outcome}" where a transport that replaced its own refuses it`, async () => {
      const heard: string[] = []
      const page = await subscribedPage(kind, heard)
      relay(before === 'never took effect' ? 'held' : 'at once').configure()
      const wanted = await broadcastWantedBy(page, kind)
      if (before !== 'never took effect') {
        await wanted.inEffect
        await (kind === 'text'
          ? wanted.broadcast.publish('live')
          : wanted.broadcast.publishBinary(new TextEncoder().encode('live')))
        await vi.waitFor(() => expect(heard).toEqual(['live']))
      }
      const refused = () => ({ unsubscribe() {}, opened: Promise.reject(new Error('NOPERM: replacement refused')) })
      const sent = () => ({ seq: 1, timestamp: 1 })
      configureBroadcastTransport({ send: sent, sendBinary: sent, listen: refused, listenBinary: refused })
      const given = await page.given()
      expect(given.onClose).toBeInstanceOf(ChannelClosedError)
      expect(given.onClose instanceof Error && given.onClose.message).toContain(outcome)
    })
  }
}

// Whether a page's subscription took effect is said of the subscription it wants now, on the plane in effect now.
for (const kind of ['text', 'binary'] as const) {
  test(`${kind}: a page's subscription still being established when a transport replaces its own is confirmed, and its call goes on, once the new transport's listener is`, async () => {
    const page = await subscribedPage(kind)
    relay('held').configure()
    const wanted = await broadcastWantedBy(page, kind)
    relay('at once').configure()
    const settled = await Promise.race([
      Promise.all([page.opened, wanted.inEffect]).then(() => 'confirmed'),
      new Promise((resolve) => setTimeout(() => resolve('waiting'), 500)),
    ])
    expect(settled).toBe('confirmed')
  })

  test(`${kind}: a page's subscription still being established across two transports that replace their own waits for the last one's listener, and its call goes on once it is`, async () => {
    const page = await subscribedPage(kind)
    const first = relay('held')
    first.configure()
    const wanted = await broadcastWantedBy(page, kind)
    const second = relay('held')
    second.configure()
    const last = relay('held')
    last.configure()
    const settled = () =>
      Promise.race([
        Promise.all([page.opened, wanted.inEffect]).then(() => 'confirmed'),
        new Promise((resolve) => setTimeout(() => resolve('waiting'), 100)),
      ])
    first.confirm()
    second.confirm()
    const beforeTheLast = await settled()
    last.confirm()
    expect({ beforeTheLast, once: await settled() }).toEqual({ beforeTheLast: 'waiting', once: 'confirmed' })
  })

  for (const first of ['at once', 'held'] as const) {
    test(`${kind}: a page's new subscription that a replacing transport refuses gets "did not take effect", though a server-side listener on its broadcast kept the earlier one, which its transport confirmed ${first === 'held' ? 'later' : 'at once'}, in effect`, async () => {
      const page = await subscribedPage(kind)
      const transport = relay(first)
      transport.configure()
      const wanted = await broadcastWantedBy(page, kind)
      transport.confirm()
      await wanted.inEffect
      const listener =
        kind === 'text' ? wanted.broadcast.subscribe(() => {}) : wanted.broadcast.subscribeBinary(() => {})
      await listener.opened
      const replacement = relay('held')
      replacement.configure()
      await wanted.want(2, false)
      void wanted.want(3, true)
      replacement.refuse()
      const given = await page.given()
      expect(given.onClose instanceof Error && given.onClose.message).toContain(
        'its subscription did not take effect on the server',
      )
    })
  }
}

// A page's subscription its page withdrew before its transport confirmed it holds nothing on the server, while the page
// stays connected and its transport never answers.
for (const kind of ['text', 'binary'] as const) {
  test(`${kind}: the subscriptions a page withdrew while its transport's first confirmation was pending hold nothing on the server`, async () => {
    const page = await subscribedPage(kind)
    relay('held').configure()
    const wanted = await broadcastWantedBy(page, kind)
    // What waits for each of the page's wants to take effect.
    const waits: WeakRef<object>[] = []
    const hook = createHook({
      init(_id, type, _trigger, resource) {
        if (type === 'PROMISE' && new Error().stack?.includes('whenInEffect')) waits.push(new WeakRef(resource))
      },
    }).enable()
    let version = 1
    for (let round = 0; round < 16; round++) {
      void wanted.want(++version, true)
      void wanted.want(++version, false)
    }
    hook.disable()
    await collectGarbage()
    expect({ waited: waits.length > 0, reachable: waits.filter((wait) => wait.deref() !== undefined).length }).toEqual({
      waited: true,
      reachable: 0,
    })
  })
}

/** A transport that delivers each publish to every listener on this server, and confirms each listener at once, or where
 *  `held` once `confirm()` confirms or `refuse()` refuses those it hasn't settled. */
function relay(confirms: 'at once' | 'held') {
  const listeners = new Set<(payload: never, info: { seq: number; timestamp: number }) => void>()
  const unconfirmed: Deferred<void>[] = []
  let seq = 0
  const listen = (_key: string, onMessage: (payload: never, info: { seq: number; timestamp: number }) => void) => {
    listeners.add(onMessage)
    const unsubscribe = () => void listeners.delete(onMessage)
    if (confirms === 'at once') return unsubscribe
    const ready = createDeferred<void>()
    unconfirmed.push(ready)
    return { unsubscribe, opened: ready.promise }
  }
  const send = (_key: string, payload: never) => {
    const info = { seq: ++seq, timestamp: 1 }
    for (const onMessage of listeners) onMessage(payload, info)
    return info
  }
  return {
    configure: () => configureBroadcastTransport({ send, sendBinary: send, listen, listenBinary: listen }),
    confirm: () => {
      for (const ready of unconfirmed.splice(0)) ready.resolve()
    },
    refuse: () => {
      for (const ready of unconfirmed.splice(0)) ready.reject(new Error('NOPERM: replacement refused'))
    },
  }
}

/** A broadcast whose first call carries `page`'s subscription of `kind`, on the transport configured now; what the server
 *  sends the page reaches it as sent, and `want()` is a later call's want of the same kind. */
async function broadcastWantedBy(page: Awaited<ReturnType<typeof subscribedPage>>, kind: 'text' | 'binary') {
  const broadcast = new ServerBroadcast<string>({ key: crypto.randomUUID() })
  const end = broadcast._end
  const call = new Request('https://example.test/_telefunc?session=page', { method: 'POST' })
  end._callRequest = call
  getChannelMux().registerChannel(end, ATTACHABLE_NOW)
  const sender: PeerSender = {
    send: (frame) => page.receive(new Uint8Array(frame)),
    carry() {},
    bufferedAmount: () => 0,
    behindAfter: () => Number.POSITIVE_INFINITY,
    deliveredBefore: () => 0,
    sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
    queue: { waitingPublishes: () => 0, dropUngiven: () => 0, dropWaitingPublishes() {}, dropWaitingFrom() {} },
    terminate() {},
  }
  const replay = end._replayBuffer
  if (!replay) throw new Error('A registered broadcast has its replay')
  end._attachPeer(new IndexedPeer({ sender, index: page.index, replay }), {})
  const want = (version: number, on: boolean) =>
    getChannelMux().applyCallWants(call, [
      { channel: broadcast.id, version, text: on && kind === 'text', binary: on && kind === 'binary' },
    ])
  return { broadcast, inEffect: want(1, true), want }
}

/** A page's broadcast, subscribed, on an SSE wire this test serves once it reconciled: `receive()` hands it a frame the
 *  server sent, and `given()` is what its `opened` rejects with and its `onClose()` callback gets. */
async function subscribedPage(kind: 'text' | 'binary' = 'text', heard: string[] = []) {
  const reconciled = createDeferred<{ index: number; push: (frame: Uint8Array<ArrayBuffer>) => void }>()
  vi.stubGlobal('fetch', async (_url: string, init: RequestInit) => {
    const { streamResponse, frames } = await batchOf(init.body)
    if (!streamResponse) return new Response('', { status: 200 })
    let index = 0
    for (const frame of frames) if (frame.tag === TAG.RECONCILE) index = frame.payload.open[0]?.ix ?? index
    const encoder = new TextEncoder()
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        const push = (frame: Uint8Array<ArrayBuffer>) =>
          controller.enqueue(encoder.encode(`data: ${uint8ArrayToBase64url(frame)}\n\n`))
        controller.enqueue(encoder.encode(': open\n\n'))
        push(encode.streamRequestOpenAck())
        push(
          encode.reconciled({ ...SERVER_OPTIONS, sessionId: crypto.randomUUID(), open: [{ ix: index, lastSeq: 0 }] }),
        )
        reconciled.resolve({ index, push })
      },
    })
    return new Response(stream, { status: 200, headers: { 'Content-Type': 'text/event-stream' } })
  })
  const page = new ClientBroadcast({
    channelId: crypto.randomUUID(),
    connection: {
      transports: [CHANNEL_TRANSPORT.SSE],
      telefuncUrl: 'http://test.local/_telefunc',
      connectionKey: crypto.randomUUID(),
    },
  })
  const onClose = new Promise<unknown>((resolve) => page.onClose(resolve))
  const subscription =
    kind === 'text'
      ? page.subscribe((message) => heard.push(String(message)))
      : page.subscribeBinary((data) => heard.push(new TextDecoder().decode(data)))
  const { index, push } = await reconciled.promise
  return {
    index,
    receive: push,
    opened: subscription.opened,
    given: async () => ({ opened: await subscription.opened.catch((err: unknown) => err), onClose: await onClose }),
  }
}

/** The ERROR frame the server sends the page at `index` whose call carries its subscription to a broadcast the call's
 *  session got, where its backend `listen()`s as given: once that refusal ends it, or once the call waited `connectTtl`. */
async function errorSentToPage(
  index: number,
  listen: () => BroadcastUnsubscribe,
  pastLimit: boolean,
): Promise<Uint8Array<ArrayBuffer>> {
  const sent = () => ({ seq: 1, timestamp: 1 })
  configureBroadcastTransport({ send: sent, sendBinary: sent, listen, listenBinary: listen })
  try {
    if (pastLimit) vi.useFakeTimers()
    const broadcast = new ServerBroadcast<string>({ key: crypto.randomUUID() })
    const end = broadcast._end
    const call = new Request('https://example.test/_telefunc?session=page', { method: 'POST' })
    // As `carryChannel` stamps the broadcast a call returns.
    end._callRequest = call
    getChannelMux().registerChannel(end, ATTACHABLE_NOW)
    const frames: Uint8Array<ArrayBuffer>[] = []
    const sender: PeerSender = {
      send: (frame) => frames.push(new Uint8Array(frame)),
      carry() {},
      bufferedAmount: () => 0,
      behindAfter: () => Number.POSITIVE_INFINITY,
      deliveredBefore: () => 0,
      sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
      queue: { waitingPublishes: () => 0, dropUngiven: () => 0, dropWaitingPublishes() {}, dropWaitingFrom() {} },
      terminate() {},
    }
    const replay = end._replayBuffer
    if (!replay) throw new Error('A registered broadcast has its replay')
    end._attachPeer(new IndexedPeer({ sender, index, replay }), {})
    const waiting = getChannelMux().applyCallWants(call, [
      { channel: broadcast.id, version: 1, text: true, binary: false },
    ])
    if (pastLimit) await vi.advanceTimersByTimeAsync(getChannelMux().connectTtl)
    await waiting
    await broadcast.closed
    const error = frames.find((frame) => decode(frame).tag === TAG.ERROR)
    if (!error) throw new Error(`The server sent its page no ERROR, only tags ${frames.map((frame) => frame[0])}`)
    return error
  } finally {
    vi.useRealTimers()
  }
}

const SERVER_OPTIONS = {
  reconnectTimeout: 60_000,
  idleTimeout: 60_000,
  pingInterval: 100_000,
  maxFrameBytes: 64 * 1024 * 1024,
  creditWindow: 1_000_000,
  sseFlushThrottle: 300,
  ssePostIdleFlushDelay: 50,
  transports: [CHANNEL_TRANSPORT.SSE],
}

/** A page's POST, read as the server's SSE handler reads it: its metadata, then its frames. */
async function batchOf(body: unknown): Promise<{ streamResponse: boolean; frames: DecodedFrame[] }> {
  if (!(body instanceof Blob)) return { streamResponse: false, frames: [] }
  // A `Blob`'s stream doesn't fail.
  const reader = new BodyReader(body.stream(), () => false, queueMemory)
  const metadata = await reader.readFrame(SSE_METADATA_MAX_BYTES)
  if (metadata === null) return { streamResponse: false, frames: [] }
  const { kind } = parseSseRequestMetadata(JSON.parse(new TextDecoder().decode(metadata)))
  const frames: DecodedFrame[] = []
  for (let frame; (frame = await reader.readFrame(WIRE_MAX_RAW_FRAME_BYTES)); ) frames.push(decode(frame))
  return { streamResponse: kind === 'streamResponse', frames }
}
