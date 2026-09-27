import { expect, test } from 'vitest'

import { ClientChannel } from './channel.js'
import { config } from '../../client/clientConfig.js'
import { CHANNEL_TRANSPORT } from '../constants.js'
import { getSessionToken } from './session-registry.js'

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
