import { afterEach, expect, test, vi } from 'vitest'

import { ClientBroadcast, ClientChannel } from './channel.js'
import { config } from '../../client/clientConfig.js'
import { CHANNEL_TRANSPORT } from '../constants.js'
import { TAG } from '../shared-ws.js'
import { getSessionToken } from './session-registry.js'

const broadcasts: ClientBroadcast[] = []
const channels: ClientChannel[] = []
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

test('a channel made before the page has a session token makes one, so the call that carries it presents the same', () => {
  config.fetch = async () => new Response(new ReadableStream({ start() {} }), { status: 200 })
  const telefuncUrl = 'http://first-call.test/_telefunc'
  expect(getSessionToken(telefuncUrl)).toBeUndefined()
  const channel = new ClientChannel({
    channelId: crypto.randomUUID(),
    transports: [CHANNEL_TRANSPORT.SSE],
    telefuncUrl,
    connectionKey: crypto.randomUUID(),
  })
  expect(getSessionToken(telefuncUrl)).toEqual(expect.any(String))
  channel.abort()
})

test('a close request goes out again when its channel re-attaches before the close is acknowledged', () => {
  config.fetch = async () => new Response(new ReadableStream({ start() {} }), { status: 200 })
  const channel = new ClientChannel({
    channelId: crypto.randomUUID(),
    transports: [CHANNEL_TRANSPORT.SSE],
    telefuncUrl: 'http://close-resend.test/_telefunc',
    connectionKey: crypto.randomUUID(),
  })
  const sendCloseRequest = vi.spyOn((channel as any)._connection, 'sendCloseRequest')
  void channel.close({ timeout: 5_000 })
  channel._onTransportOpen(false) // the reconcile of a reconnect: the first request may have died with the old wire
  expect(sendCloseRequest).toHaveBeenCalledTimes(2)
})

test('a close the server acknowledged ends gracefully, though a reconnect then drops the channel', async () => {
  config.fetch = async () => new Response(new ReadableStream({ start() {} }), { status: 200 })
  const channel = new ClientChannel({
    channelId: crypto.randomUUID(),
    transports: [CHANNEL_TRANSPORT.SSE],
    telefuncUrl: 'http://close-acked.test/_telefunc',
    connectionKey: crypto.randomUUID(),
  })
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
    })
  expect(seen).toEqual(['once:one', 'other:one', 'other:two'])
})
