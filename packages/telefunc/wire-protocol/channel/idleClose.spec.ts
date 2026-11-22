// A page that closes its connection for good with no `pagehide`: its last channel closed, the connection closes once
// idle. Over a real server and page, on each transport; and the SSE wire a page leaves for a WebSocket, which isn't its
// connection closing.

import { afterAll, beforeAll, expect, test } from 'vitest'
import { createServer, type Server } from 'node:http'
import type { Duplex } from 'node:stream'
import { setFlagsFromString } from 'node:v8'
import { runInNewContext } from 'node:vm'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { config as clientConfig } from '../../client/clientConfig.js'
import { ServerChannel } from './server/ServerChannel.js'
import { ClientChannel } from './client/ClientChannel.js'
import { getChannelMux } from './server/ChannelMux.js'

// The connection's idle timeout, half a second, so that a page whose last channel closed closes its connection soon.
serverConfig.channel = { transports: ['sse', 'ws'], idleTimeout: 500 }
serverConfig.disableNamingConvention = true

let server: Server
let telefuncUrl: string
/** The page's requests and WebSockets the server has open. */
let requests = 0
const upgraded = new Set<Duplex>()
/** The `close` requests pages sent: an SSE wire's, which tells the server the page left. */
let closes = 0
clientConfig.fetch = async (input, init) => {
  if (init?.body instanceof Blob && (await init.body.text()).includes('"close":true')) closes++
  return fetch(input, init)
}

beforeAll(async () => {
  const telefunc = new Telefunc()
  server = createServer((req, res) => {
    requests++
    res.once('close', () => requests--)
    void telefunc.serve({ req, res })
  })
  server.on('upgrade', (_req, socket: Duplex) => {
    upgraded.add(socket)
    socket.once('close', () => upgraded.delete(socket))
  })
  telefunc.installWebSocket(server)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  telefuncUrl = `http://127.0.0.1:${address.port}/_telefunc`
})
afterAll(() => {
  server.closeAllConnections()
  server.close()
})

setFlagsFromString('--expose-gc')
const gc: unknown = runInNewContext('gc')

for (const transport of ['ws', 'sse'] as const)
  for (const closer of ['page', 'server'] as const) {
    test(`over ${transport === 'ws' ? 'a WebSocket' : 'SSE'}, a page whose last channel its ${closer} closed, its connection then closed for good, is let go by the server at once`, async () => {
      /** A page's one channel, closed by `closer`: the server's channel, held only weakly here. */
      async function closed() {
        const channel = new ServerChannel<string, string>()
        channel.listen((data) => `ack:${data}`)
        getChannelMux().registerChannel(channel)
        const page = new ClientChannel<string, string>({
          channelId: channel.id,
          connection: { telefuncUrl, transports: [transport], connectionKey: crypto.randomUUID() },
        })
        expect(await page.send('page', { ack: true })).toBe('ack:page')
        if (closer === 'page') await page.close()
        else await channel.close()
        await Promise.all([page.closed, channel.closed])
        return new WeakRef(channel)
      }
      const channel = await closed()
      // Its connection closes once idle: no request or WebSocket of it is left on the server.
      await expect.poll(() => requests + upgraded.size, { timeout: 5_000 }).toBe(0)
      // Well inside `reconnectTimeout`, 60 s by default, for which a page that may come back is waited for.
      await expect
        .poll(
          () => {
            if (typeof gc === 'function') gc()
            return channel.deref() === undefined
          },
          { timeout: 2_000, interval: 50 },
        )
        .toBe(true)
    }, 15_000)
  }

test('over SSE, the wire a page leaves for a WebSocket by its upgrade tells the server nothing of a close', async () => {
  const channel = new ServerChannel<string, string>()
  channel.listen((data) => `ack:${data}`)
  getChannelMux().registerChannel(channel)
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    connection: { telefuncUrl, transports: ['sse', 'ws'], connectionKey: crypto.randomUUID() },
  })
  const before = closes
  expect(await page.send('sse', { ack: true })).toBe('ack:sse')
  // On its WebSocket, with no request of its SSE wire left on the server.
  await expect
    .poll(() => ({ requests, upgraded: upgraded.size }), { timeout: 5_000 })
    .toEqual({ requests: 0, upgraded: 1 })
  expect(await page.send('ws', { ack: true })).toBe('ack:ws')
  expect(closes - before).toBe(0)
  await page.close()
  await Promise.all([page.closed, channel.closed])
}, 15_000)
