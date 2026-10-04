import { afterEach, expect, test, vi } from 'vitest'

import { ClientBroadcast, ClientChannel } from './channel.js'
import { config } from '../../client/clientConfig.js'
import { CHANNEL_TRANSPORT } from '../constants.js'
import { TAG, decode, encode, encodePublishBinary, encodePublishText, type ChannelFrame } from '../shared-ws.js'
import { getSessionUrl } from './session-registry.js'

const broadcasts: ClientBroadcast[] = []
const channels: ClientChannel<never, string>[] = []
afterEach(() => {
  for (const broadcast of broadcasts.splice(0)) broadcast.abort()
  for (const channel of channels.splice(0)) channel.abort()
  delete config.fetch
  vi.restoreAllMocks()
})

/** A ClientChannel whose wire never opens, so the test hands it each frame. */
function stalledChannel(): ClientChannel<never, string> {
  config.fetch = async () => new Response(new ReadableStream({ start() {} }), { status: 200 })
  const channel = new ClientChannel<never, string>({
    channelId: crypto.randomUUID(),
    transports: [CHANNEL_TRANSPORT.SSE],
    telefuncUrl: 'http://client-channel.test/_telefunc',
    connectionKey: crypto.randomUUID(),
  })
  channels.push(channel)
  return channel
}

test('close() refuses a timeout longer than a timer waits, which would fire at once', () => {
  const channel = stalledChannel()
  expect(() => channel.close({ timeout: 2 ** 31 })).toThrow('at most 2147483647')
  expect(channel.isClosed).toBe(false)
})

test("a channel listener that stops listening itself doesn't make the next one miss the message", () => {
  const channel = stalledChannel()
  const seen: string[] = []
  const unlisten = channel.listen((message) => {
    seen.push(`once:${message}`)
    unlisten()
  })
  channel.listen((message) => void seen.push(`other:${message}`))
  for (const [seq, text] of [
    [1, 'one'],
    [2, 'two'],
  ] as const)
    channel._dispatchFrame({ tag: TAG.TEXT, index: 0, seq, text: JSON.stringify(text), bytes: 5 })
  expect(seen).toEqual(['once:one', 'other:one', 'other:two'])
})

test('a channel made before the page has a session token names one, which the call that carries it presents too', async () => {
  const requested: string[] = []
  config.fetch = async (url) => {
    requested.push(String(url))
    return new Response(new ReadableStream({ start() {} }), { status: 200 })
  }
  const telefuncUrl = 'http://first-call.test/_telefunc'
  channels.push(
    new ClientChannel({
      channelId: crypto.randomUUID(),
      transports: [CHANNEL_TRANSPORT.SSE],
      telefuncUrl,
      connectionKey: crypto.randomUUID(),
    }),
  )
  await vi.waitFor(() => expect(requested).not.toEqual([]))
  const session = new URL(requested[0]!).searchParams.get('session')
  expect(getSessionUrl(telefuncUrl)).toBe(`${telefuncUrl}?session=${session}`)
})

test('a close the server acknowledged ends gracefully, though a reconnect then drops the channel', async () => {
  const channel = stalledChannel()
  const closedWith: unknown[] = []
  channel.onClose((err) => void closedWith.push(err))
  const closing = channel.close({ timeout: 5_000 })
  channel._onTransportCloseAck()
  // An upgrade's RECONCILED, applied in the same turn, no longer lists the channel the server closed.
  channel._onTransportClose(new Error('Channel not acknowledged by server after reconnect'))
  expect(await closing).toBe(0)
  expect(closedWith).toEqual([undefined])
})

test('a channel opens on a page served over plain http, which has no crypto.randomUUID()', () => {
  config.fetch = async () => new Response(new ReadableStream({ start() {} }), { status: 200 })
  const channelId = crypto.randomUUID()
  const connectionKey = crypto.randomUUID()
  vi.spyOn(crypto, 'randomUUID').mockImplementation(() => {
    throw new TypeError('crypto.randomUUID is not a function')
  })
  const telefuncUrl = 'http://192.168.1.2:3000/_telefunc'
  expect(
    () => new ClientChannel({ channelId, transports: [CHANNEL_TRANSPORT.SSE], telefuncUrl, connectionKey }),
  ).not.toThrow()
})

/** A ClientBroadcast whose wire never opens, so the test hands it each frame. */
function stalledBroadcast(): ClientBroadcast {
  config.fetch = async () => new Response(new ReadableStream({ start() {} }), { status: 200 })
  const broadcast = new ClientBroadcast({
    channelId: crypto.randomUUID(),
    key: 'client-broadcast',
    transports: [CHANNEL_TRANSPORT.SSE],
    telefuncUrl: 'http://client-broadcast.test/_telefunc',
    connectionKey: crypto.randomUUID(),
  })
  broadcasts.push(broadcast)
  return broadcast
}

test("a subscriber that unsubscribes itself doesn't make the next one miss the message", () => {
  const broadcast = stalledBroadcast()
  const seen: string[] = []
  const off = broadcast.subscribe((message) => {
    seen.push(`once:${String(message)}`)
    off()
  })
  broadcast.subscribe((message) => void seen.push(`other:${String(message)}`))
  for (const [seq, text] of [
    [1, 'one'],
    [2, 'two'],
  ] as const)
    broadcast._dispatchFrame({
      tag: TAG.PUBLISH,
      index: 0,
      seq,
      text: JSON.stringify(text),
      info: { seq, timestamp: 1 },
      bytes: 0,
    })
  expect(seen).toEqual(['once:one', 'other:one', 'other:two'])
})

test('a broadcast delivers each publish with the seq its key was given, to text and binary subscribers alike, past 2^32', () => {
  const broadcast = stalledBroadcast()
  const seen: number[] = []
  broadcast.subscribe((_, info) => void seen.push(info.seq))
  broadcast.subscribeBinary((_, info) => void seen.push(info.seq))
  const seqs = [2 ** 32 - 1, 2 ** 32, 2 ** 32 + 1, 2 ** 32 + 2, Number.MAX_SAFE_INTEGER - 1, Number.MAX_SAFE_INTEGER]
  // As the server sends each: alternately text and binary.
  for (const [n, seq] of seqs.entries()) {
    const info = { seq, timestamp: 1 }
    const frame =
      n % 2 === 0
        ? encode.publish(0, encodePublishText('"x"', info), n + 1)
        : encode.publishBinary(0, encodePublishBinary(new Uint8Array([1]), info), n + 1)
    broadcast._dispatchFrame(decode(frame) as ChannelFrame)
  }
  expect(seen).toEqual(seqs)
})

test('a broadcast declares its subscriptions on every attach, as a subscribe written to a wire already dead is lost', () => {
  const broadcast = stalledBroadcast()
  broadcast.subscribe(() => {})
  const offBinary = broadcast.subscribeBinary(() => {})
  expect(broadcast._reattachState()).toEqual({ broadcast: { text: true, binary: true } })
  offBinary()
  expect(broadcast._reattachState()).toEqual({ broadcast: { text: true, binary: false } })
})

test("a broadcast's toggles after close() send nothing, so none can hold its channel for a reconcile that would re-attach it", () => {
  const broadcast = stalledBroadcast()
  const off = broadcast.subscribe(() => {})
  void broadcast.close().catch(() => {})
  off()
  broadcast.subscribeBinary(() => {})
  const tags = (broadcast as any)._connection.sendBuffer.map(({ frame }: { frame: Uint8Array }) => frame[0])
  expect(tags.filter((tag: number) => tag === TAG.BROADCAST_UNSUB || tag === TAG.BROADCAST_SUB)).toEqual([
    TAG.BROADCAST_SUB,
  ])
})

test('a broadcast subscribes the page to a kind with its first listener, and unsubscribes it with the last', () => {
  const broadcast = stalledBroadcast()
  broadcast.subscribeBinary(() => {})()
  const frames = (broadcast as any)._connection.sendBuffer.map(({ frame }: { frame: Uint8Array }) => decode(frame))
  expect(frames).toMatchObject([
    { tag: TAG.BROADCAST_SUB, binary: true },
    { tag: TAG.BROADCAST_UNSUB, binary: true },
  ])
})
