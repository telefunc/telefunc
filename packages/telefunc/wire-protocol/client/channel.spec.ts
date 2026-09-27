import { afterEach, expect, test } from 'vitest'

import { ClientChannel } from './channel.js'
import { config } from '../../client/clientConfig.js'
import { CHANNEL_TRANSPORT } from '../constants.js'
import { TAG } from '../shared-ws.js'
import { getSessionToken } from './session-registry.js'

const channels: ClientChannel[] = []
afterEach(() => {
  for (const channel of channels.splice(0)) channel.abort()
  delete config.fetch
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
