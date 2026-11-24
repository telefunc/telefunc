// Telefunc's refusal of a request whose `x-telefunc-request` header is missing, as behind a proxy that drops it, is
// marked `route` on the Node adapter as on Cloudflare's, so a page leaves SSE rather than retry: the real Node server
// and the real page (decisions/516-sse-carrier.md, the ruling on step 6's check, R1.3).

import { afterAll, beforeAll, expect, test } from 'vitest'
import { createServer, type Server } from 'node:http'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { STATUS_BODY_MALFORMED_REQUEST } from '../../shared/constants.js'
import { REQUEST_KIND, REQUEST_KIND_HEADER, getMarkedRequestUrl } from '../wire/requestKind.js'
import { ClientChannel } from './client/ClientChannel.js'
import { getWireUrl } from './client/getSessionUrl.js'
import { getChannelMux } from './server/ChannelMux.js'
import { ServerChannel, ATTACHABLE_NOW } from './server/ServerChannel.js'

serverConfig.channel = { transports: ['sse', 'ws'] }

/** Each answer to a request the proxy passed on without its `x-telefunc-request` header: status and media type. */
const stripped: { status: number; contentType: string | null }[] = []
const realFetch = globalThis.fetch
/** A proxy that drops the header from every request it passes on. */
async function strippingFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  const headers = new Headers(init?.headers)
  headers.delete(REQUEST_KIND_HEADER)
  const response = await realFetch(input, { ...init, headers })
  stripped.push({ status: response.status, contentType: response.headers.get('content-type') })
  return response
}

let server: Server
let telefuncUrl: string

beforeAll(async () => {
  const telefunc = new Telefunc()
  server = createServer((req, res) => void telefunc.serve({ req, res }))
  telefunc.installWebSocket(server)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  telefuncUrl = `http://127.0.0.1:${address.port}/_telefunc`
})
afterAll(() => {
  globalThis.fetch = realFetch
  server.closeAllConnections()
  server.close()
})

/** Resolves once `done()` holds, checked every 10 ms; rejects after `ms`. */
async function until(done: () => boolean, ms = 3_500): Promise<void> {
  const deadline = performance.now() + ms
  while (!done()) {
    if (performance.now() > deadline) throw new Error('timed out waiting')
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
}

const route = /^text\/vnd\.telefunc\.refusal; kind=route; reason="Malformed Telefunc Request"$/

test('an SSE request without its `x-telefunc-request` header is refused `route`, its body what a telefunction call reads', async () => {
  const response = await realFetch(getMarkedRequestUrl(getWireUrl(telefuncUrl), REQUEST_KIND.SSE), {
    method: 'POST',
    body: new Uint8Array(8),
  })
  expect(response.status).toBe(400)
  expect(response.headers.get('content-type')).toMatch(route)
  expect(await response.text()).toBe(STATUS_BODY_MALFORMED_REQUEST)
})

test("behind a proxy that drops `x-telefunc-request`, a page with `['sse', 'ws']` moves to a WebSocket, and its message arrives", async () => {
  globalThis.fetch = strippingFetch
  const channel = new ServerChannel<string, string>()
  getChannelMux().registerChannel(channel, ATTACHABLE_NOW)
  const got: string[] = []
  channel.listen((message) => void got.push(message))
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    connection: { telefuncUrl, transports: ['sse', 'ws'], idleTimeout: 0, connectionKey: crypto.randomUUID() },
  })
  const pageEnd: Error[] = []
  page.onClose((err) => void pageEnd.push(err ?? new Error('closed with no error')))
  void page.send('over the WebSocket').catch(() => {})
  await until(() => got.length > 0 || pageEnd.length > 0)
  expect(pageEnd).toEqual([])
  expect(got).toEqual(['over the WebSocket'])
  expect(stripped).toEqual([{ status: 400, contentType: expect.stringMatching(route) }])
})
