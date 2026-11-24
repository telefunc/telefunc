// What a page is given when its broadcast's subscription doesn't take effect on the server: the REAL ChannelMux and
// ServerBroadcast end the broadcast, and the ERROR frame they send its page reaches the REAL ClientBroadcast, as sent, over
// an SSE wire this test serves.

import { afterEach, expect, test, vi } from 'vitest'

import { ClientBroadcast } from './client/ClientBroadcast.js'
import { getChannelMux } from './server/ChannelMux.js'
import { ServerBroadcast } from './server/ServerBroadcast.js'
import { IndexedPeer, type PeerSender } from './server/IndexedPeer.js'
import { ChannelClosedError } from './channelErrors.js'
import {
  DefaultBroadcastAdapter,
  _resetBroadcastAdapterForTesting,
  getBroadcastAdapter,
  type BroadcastUnsubscribe,
} from '../broadcast/broadcastAdapter.js'
import { decode, encode, type DecodedFrame } from '../wire/frame.js'
import { TAG } from '../wire/tags.js'
import { parseSseRequestMetadata } from '../wire/envelope.js'
import { BodyReader } from '../wire/BodyReader.js'
import { uint8ArrayToBase64url } from '../wire/base64url.js'
import { CHANNEL_TRANSPORT, SSE_METADATA_MAX_BYTES, WIRE_MAX_RAW_FRAME_BYTES } from '../constants.js'
import { createDeferred } from '../../utils/createDeferred.js'
import { ATTACHABLE_NOW } from './server/ServerChannel.js'

afterEach(() => {
  vi.unstubAllGlobals()
  vi.useRealTimers()
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

/** A page's broadcast, subscribed, on an SSE wire this test serves once it reconciled: `receive()` hands it a frame the
 *  server sent, and `given()` is what its `opened` rejects with and its `onClose()` callback gets. */
async function subscribedPage() {
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
  const subscription = page.subscribe(() => {})
  const { index, push } = await reconciled.promise
  return {
    index,
    receive: push,
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
  const previous = getBroadcastAdapter()
  _resetBroadcastAdapterForTesting(
    new DefaultBroadcastAdapter({ send: sent, sendBinary: sent, listen, listenBinary: listen }),
  )
  try {
    if (pastLimit) vi.useFakeTimers()
    const broadcast = new ServerBroadcast<string>({ key: crypto.randomUUID() })
    const call = new Request('https://example.test/_telefunc?session=page', { method: 'POST' })
    // As `carryChannel` stamps the broadcast a call returns.
    broadcast._callRequest = call
    getChannelMux().registerChannel(broadcast, ATTACHABLE_NOW)
    const frames: Uint8Array<ArrayBuffer>[] = []
    const sender: PeerSender = {
      send: (frame) => frames.push(new Uint8Array(frame)),
      carry() {},
      bufferedAmount: () => 0,
      behindAfter: () => Number.POSITIVE_INFINITY,
      deliveredBefore: () => 0,
      sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
      queue: { waitingPublishes: () => 0, dropUngiven: () => 0, dropWaitingPublishes() {}, dropWaitingFrom() {} },
    }
    const replay = broadcast._replayBuffer
    if (!replay) throw new Error('A registered broadcast has its replay')
    broadcast._attachPeer(new IndexedPeer({ sender, index, replay }), {})
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
    _resetBroadcastAdapterForTesting(previous)
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
  const reader = new BodyReader(body.stream(), () => false)
  const metadata = await reader.readFrame(SSE_METADATA_MAX_BYTES)
  if (metadata === null) return { streamResponse: false, frames: [] }
  const { kind } = parseSseRequestMetadata(JSON.parse(new TextDecoder().decode(metadata)))
  const frames: DecodedFrame[] = []
  for (let frame; (frame = await reader.readFrame(WIRE_MAX_RAW_FRAME_BYTES)); ) frames.push(decode(frame))
  return { streamResponse: kind === 'streamResponse', frames }
}
