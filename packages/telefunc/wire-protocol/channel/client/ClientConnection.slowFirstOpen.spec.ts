// A page's first SSE open on the slowest link it supports, through a proxy that forwards a request only once its body
// arrived whole (nginx's default `proxy_request_buffering on`): an open that carries a large RECONCILE is answered only
// after its body crossed the link, and is given that time; one whose body is small is given up on as before. The real
// Telefunc server, the real page, on the scripted server's virtual clock (scriptedServer.fixture.ts).

import { expect, test, vi } from 'vitest'
import { Telefunc } from '../../../serve/node.js'
import { config as serverConfig } from '../../../node/server/serverConfig.js'
import { getChannelMux } from '../server/ChannelMux.js'
import { ATTACHABLE_NOW, ServerChannel } from '../server/ServerChannel.js'
import { ClientChannel } from './ClientChannel.js'
import { advance, useScriptedServer, world } from './scriptedServer.fixture.js'

/** 400 kbit/s, and a 400 ms round trip. */
const BYTES_PER_MS = 50
const ROUND_TRIP_MS = 400

serverConfig.channel = { transports: ['sse'] }
useScriptedServer()

test("1,000 channels of 256-byte ids open on a page's first SSE wire at 400 kbit/s behind a proxy that buffers the request body, within the server's connect window", async () => {
  const app = new Telefunc()
  serverConfig.telefuncUrl = new URL(world.telefuncUrl).pathname
  vi.stubGlobal('fetch', async (input: RequestInfo | URL, init?: RequestInit) => {
    // No streaming upload over HTTP/1.1: the page sends in batch POSTs.
    if (init?.body instanceof ReadableStream) throw new TypeError('Failed to fetch')
    const { body, signal } = init ?? {}
    if (body instanceof Blob) {
      // The proxy forwards the request once its body crossed the link; the answer's head takes the way back.
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(resolve, ROUND_TRIP_MS + body.size / BYTES_PER_MS)
        signal?.addEventListener(
          'abort',
          () => {
            clearTimeout(timer)
            reject(new DOMException('aborted', 'AbortError'))
          },
          { once: true },
        )
      })
    }
    const response = await app.serve({ request: new Request(input, init) })
    if (!response) throw new Error('not served')
    return response
  })
  const expired: string[] = []
  const pages = Array.from({ length: 1_000 }, (_, i) => {
    const channel = new ServerChannel({ id: String(i).padStart(4, '0') + 'x'.repeat(252) })
    getChannelMux().registerChannel(channel, ATTACHABLE_NOW)
    channel.onClose((err) => void (err && expired.push(err.message)))
    return new ClientChannel({
      channelId: channel.id,
      connection: { transports: ['sse'], telefuncUrl: world.telefuncUrl, connectionKey: 'slow first open' },
    })
  })
  let opened = 0
  for (const page of pages)
    void page.opened.then(
      () => opened++,
      () => {},
    )
  await advance(15_100)
  expect({ opened, expired }).toEqual({ opened: 1_000, expired: [] })
})

test('unanswered SSE opens are cut at 3 s, then 6 s, and keep an upload refusal', async () => {
  vi.spyOn(Math, 'random').mockReturnValue(0.5)
  type Open = { at: number; endedAfter: number | null }
  const opens: Open[] = []
  let uploads = 0
  vi.stubGlobal('fetch', (_input: RequestInfo | URL, init?: RequestInit) => {
    if (init?.body instanceof ReadableStream) {
      uploads++
      return Promise.resolve(new Response(null, { status: 413 }))
    }
    const open: Open = { at: performance.now(), endedAfter: null }
    opens.push(open)
    return new Promise<Response>((_resolve, reject) => {
      init?.signal?.addEventListener(
        'abort',
        () => {
          open.endedAfter = performance.now() - open.at
          reject(new DOMException('aborted', 'AbortError'))
        },
        { once: true },
      )
    })
  })
  const page = new ClientChannel({
    channelId: 'silent',
    connection: { transports: ['sse', 'ws'], telefuncUrl: world.telefuncUrl, connectionKey: 'silent server' },
  })
  void page.opened.catch(() => {})
  try {
    await advance(9_500)
    // Each open goes 10 ms into its attempt (`SSE_RECONCILE_DEADLINE_MS`).
    expect(opens.slice(0, 2).map(({ endedAfter }) => endedAfter)).toEqual([2_990, 5_990])
    expect(uploads).toBe(1)
  } finally {
    page.abort()
  }
})
