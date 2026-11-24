import { afterEach, expect, test } from 'vitest'
import { createServer, type Server } from 'node:http'
import type { Socket } from 'node:net'
import { Telefunc } from './node.js'
import { config as serverConfig } from '../node/server/serverConfig.js'
import { nodeReadableToWebRequest } from '../utils/nodeReadableToWebRequest.js'
import { rowOfPages, type Page } from './page.fixture.js'

// A page whose close it began and its server acknowledged sends its own CLOSE_DONE as its `close()` resolves, which the
// server confirms at the next PONG: a wire lost before that is a verdict not delivered, which the page retries. Its
// connection, idle once the verdict arrived, ends at its server's idle timeout, here half a second.

serverConfig.channel = { transports: ['sse', 'ws'], idleTimeout: 500 }

const row = rowOfPages()
afterEach(async () => {
  await row.end()
  expect(row.left.splice(0)).toEqual([])
})

/** An app that serves Telefunc with `serve({ request })`, with Telefunc's WebSocket where `webSocket`; `drop()` destroys
 *  its connections, `close()` closes it too. */
async function app(webSocket: boolean): Promise<{ host: string; drop: () => void; close: () => void }> {
  const telefunc = new Telefunc()
  const server: Server = createServer(async (req, res) => {
    const { request } = await nodeReadableToWebRequest(
      req,
      `http://${req.headers.host}${req.url}`,
      req.method ?? 'GET',
      req.headers,
    )
    const response = await telefunc.serve({ request })
    if (!response) return void res.writeHead(404).end()
    res.writeHead(response.status, Object.fromEntries(response.headers))
    res.flushHeaders()
    if (!response.body) return void res.end()
    const reader = response.body.getReader()
    try {
      for (;;) {
        const { done, value } = await reader.read()
        if (done) break
        res.write(value)
      }
      res.end()
    } catch {
      res.destroy()
    }
  })
  if (webSocket) telefunc.installWebSocket(server)
  const sockets = new Set<Socket>()
  server.on('connection', (socket: Socket) => {
    sockets.add(socket)
    socket.once('close', () => sockets.delete(socket))
  })
  row.closers.push(row.closerOf(server))
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  const drop = () => {
    for (const socket of sockets) socket.destroy()
  }
  return { host: `127.0.0.1:${address.port}`, drop, close: () => (server.close(), drop()) }
}

/** What the page's realm holds open that could start a request: its timers and its sockets. */
async function heldOpen(page: Page): Promise<{ open: string[]; pageHideListeners: number }> {
  const { open, pageHideListeners } = await page.resources()
  return { open: open.filter((kind) => /Timeout|Immediate|TCP|Pipe/.test(kind)), pageHideListeners }
}

for (const transport of ['sse', 'ws'] as const) {
  const over = transport === 'sse' ? 'SSE' : 'a WebSocket'

  test(`over ${over}, a page whose close its server acknowledged, its server gone the moment the close ended there, retries its verdict`, async () => {
    const { host, close } = await app(transport === 'ws')
    let goneAt = 0
    let serverOutcome: unknown = 'pending'
    const page = await row.page(host, {
      idleTimeout: null,
      onServerEnd: (outcome) => {
        serverOutcome = outcome
        goneAt = Date.now()
        close()
      },
    })
    expect(await page.send('page')).toBe('ack:page')
    if (transport === 'ws') expect(await page.moved(10_000)).toBe(true)
    expect(await page.close()).toBe(null)
    // From the server's end on: the page's attempts before it don't retry the verdict.
    await expect
      .poll(async () => (goneAt === 0 ? 0 : (await page.attempts()).filter(({ at }) => at >= goneAt).length), {
        timeout: 10_000,
      })
      .toBeGreaterThan(0)
    expect(serverOutcome).toBe(null)
  })

  test(`over ${over}, a page whose close its server acknowledged, its wire dropped the moment the close ended there, reconnects once, is let go, and its server's channel ended gracefully`, async () => {
    const { host, drop } = await app(transport === 'ws')
    let droppedAt = 0
    let serverOutcome: unknown = 'pending'
    // Its first reconnect at once, so that it reaches the server well before the server's idle timeout.
    const page = await row.page(host, {
      idleTimeout: null,
      random: 0,
      onServerEnd: (outcome) => {
        serverOutcome = outcome
        droppedAt = Date.now()
        drop()
      },
    })
    expect(await page.send('page')).toBe('ack:page')
    if (transport === 'ws') expect(await page.moved(10_000)).toBe(true)
    expect(await page.close()).toBe(null)
    await expect.poll(() => heldOpen(page), { timeout: 10_000 }).toEqual({ open: [], pageHideListeners: 0 })
    expect(serverOutcome).toBe(null)
    const reconnect = (await page.attempts()).filter(({ at }) => at >= droppedAt).map(({ what }) => what)
    // Over SSE: the streamed upload, the open, the first PING's batch, then the goodbye.
    expect(reconnect).toEqual(transport === 'sse' ? ['POST', 'POST', 'POST', 'POST keepalive'] : ['WebSocket'])
  })
}
