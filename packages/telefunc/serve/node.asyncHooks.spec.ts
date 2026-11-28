import { expect, test } from 'vitest'
import { executionAsyncId } from 'node:async_hooks'
import { once } from 'node:events'
import { createServer, get, Server } from 'node:http'
import { serve as honoServe } from '@hono/node-server'
import { Hono } from 'hono'
import { Telefunc } from './node.js'
import { getChannelMux } from '../wire-protocol/channel/server/ChannelMux.js'
import { ServerChannel } from '../wire-protocol/channel/server/ServerChannel.js'
import { ClientChannel } from '../wire-protocol/channel/client/ClientChannel.js'

// Its own file, so its own process: on Node 22 the first `AsyncLocalStorage.run()` of a process turns on an async hook
// for the whole process, after which every promise of it gets an async id.

test("an app on `serve({ req, res })` that doesn't call `installWebSocket()` turns on no async hook: its promises get no async id", async () => {
  const telefunc = new Telefunc()
  const server = createServer(async (req, res) => {
    if (!(await telefunc.serve({ req, res }))) res.end('the app')
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  // A route of the app's: its requests reach `serve()`, which takes the server's upgrades by itself.
  for (const _ of [1, 2]) {
    await new Promise<void>((resolve) =>
      get(`http://127.0.0.1:${address.port}/page`, (response) => response.resume().once('end', resolve)),
    )
  }
  server.close()
  await once(server, 'close')
  expect(await Promise.all([1, 2].map(() => Promise.resolve().then(() => executionAsyncId())))).toEqual([0, 0])
})

test('an app on `serve({ request })` that calls `installWebSocket()` turns on no async hook: its promises get no async id', async () => {
  const telefunc = new Telefunc()
  const app = new Hono()
  app.all('/_telefunc', async (c) => (await telefunc.serve({ request: c.req.raw })) ?? c.notFound())
  app.get('/page', (c) => c.text('the app'))
  const server = honoServe({ fetch: app.fetch, port: 0, hostname: '127.0.0.1' })
  if (!(server instanceof Server)) throw new Error('@hono/node-server made no `http.Server`')
  telefunc.installWebSocket(server)
  await once(server, 'listening')
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  // A route of the app's, then a page's channel over SSE: its requests reach `serve({ request })`.
  const page = await (await fetch(`http://127.0.0.1:${address.port}/page`)).text()
  expect(page).toBe('the app')
  const serverChannel = new ServerChannel<string, string>()
  serverChannel.listen((data) => `ack:${data}`)
  getChannelMux().registerChannel(serverChannel)
  const client = new ClientChannel<string, string>({
    channelId: serverChannel.id,
    connection: {
      telefuncUrl: `http://127.0.0.1:${address.port}/_telefunc`,
      transports: ['sse'],
      idleTimeout: 0,
      connectionKey: crypto.randomUUID(),
    },
  })
  expect(await client.send('page', { ack: true })).toBe('ack:page')
  await client.close()
  server.close()
  server.closeAllConnections()
  await once(server, 'close')
  expect(await Promise.all([1, 2].map(() => Promise.resolve().then(() => executionAsyncId())))).toEqual([0, 0])
})
