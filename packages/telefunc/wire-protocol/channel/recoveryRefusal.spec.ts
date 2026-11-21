// What a page's channels end with when a recovery gives up after a proxy refused one of its attempts. Over a real
// server and page, on SSE through a TCP forwarder the spec can cut.

import { afterAll, beforeAll, expect, test } from 'vitest'
import { createServer, type Server } from 'node:http'
import net, { type Socket } from 'node:net'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { ServerChannel } from './server/ServerChannel.js'
import { ClientChannel } from './client/ClientChannel.js'
import { getChannelMux } from './server/ChannelMux.js'

// The page gives up past two and a half ping intervals and `reconnectTimeout` over SSE: 3.5 s.
serverConfig.channel = { transports: ['sse'], pingInterval: 1_000, reconnectTimeout: 1_000 }

let server: Server
let port: number
beforeAll(async () => {
  const telefunc = new Telefunc()
  server = createServer((req, res) => void telefunc.serve({ req, res }))
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  port = address.port
})
afterAll(() => {
  server.closeAllConnections()
  server.close()
})

const BAD_GATEWAY =
  'HTTP/1.1 502 Bad Gateway\r\nContent-Type: text/html\r\nContent-Length: 15\r\nConnection: close\r\n\r\n<!doctype html>'

/** A page and its server channel through a forwarder that, once the page's wire is cut, answers the page's first
 *  event-stream open with a 502 and then does with every later connection what `then` says: reset it, or forward it
 *  as if this page spoke another wire version. What the page's channel ended with, and whether the 502 was sent. */
async function refusedOnce(then: 'reset' | 'otherWireVersion') {
  const sockets = new Set<Socket>()
  let refusing = false
  let refused = false
  const forward = (page: Socket, head: Buffer) => {
    const upstream = net.connect(port, '127.0.0.1')
    upstream.on('error', () => {})
    upstream.write(head)
    page.pipe(upstream)
    upstream.pipe(page)
    page.on('close', () => upstream.destroy())
    upstream.on('close', () => page.destroy())
  }
  const forwarder = net.createServer((page) => {
    page.on('error', () => {})
    sockets.add(page)
    page.on('close', () => sockets.delete(page))
    page.once('data', (head: Buffer) => {
      if (!refusing) return forward(page, head)
      if (!refused && head.includes('text/event-stream')) {
        refused = true
        return void page.end(BAD_GATEWAY)
      }
      if (then === 'reset' || !refused) return void page.destroy()
      forward(page, Buffer.from(head.toString('latin1').replace(/([?&]wire=)\d+/, '$10'), 'latin1'))
    })
  })
  await new Promise<void>((resolve) => forwarder.listen(0, '127.0.0.1', resolve))
  const address = forwarder.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  const channel = new ServerChannel<string, string>()
  getChannelMux().registerChannel(channel)
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    connection: {
      telefuncUrl: `http://127.0.0.1:${address.port}/_telefunc`,
      transports: ['sse'],
      connectionKey: crypto.randomUUID(),
    },
  })
  try {
    await Promise.all([channel.opened, page.opened])
    refusing = true
    for (const socket of sockets) socket.destroy()
    return { closed: await page.closed, refused }
  } finally {
    for (const socket of sockets) socket.destroy()
    forwarder.close()
  }
}

test("over SSE, a recovery whose first event-stream open a proxy answers with 502, and whose every later connection it resets, ends its channel with the 502's words", async () => {
  const { closed, refused } = await refusedOnce('reset')
  expect(refused).toBe(true)
  expect(closed).toMatchObject({ name: 'NetworkError', message: 'Server rejected SSE connection with 502 text/html' })
}, 10_000)

test('over SSE, a recovery whose next open after a 502 reaches a server that speaks another wire version ends with that, not the 502', async () => {
  const { closed, refused } = await refusedOnce('otherWireVersion')
  expect(refused).toBe(true)
  expect(closed).toMatchObject({
    name: 'NetworkError',
    message: 'The server was updated since this page loaded: reload the page.',
  })
}, 10_000)
