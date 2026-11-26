// A page's join on a live SSE connection whose uplink is batch POSTs, over a real server: the POST carrying the join's
// RECONCILE goes unanswered, as behind a proxy that holds it, while the event stream stays live.

import { afterAll, beforeAll, expect, test, vi } from 'vitest'
import { createServer, type Server } from 'node:http'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { CHANNEL_CONNECT_TTL_MS } from '../constants.js'
import { decode } from '../wire/frame.js'
import { TAG } from '../wire/tags.js'
import { ATTACHABLE_NOW, ServerChannel } from './server/ServerChannel.js'
import { ChannelMux, getChannelMux } from './server/ChannelMux.js'
import { ClientChannel } from './client/ClientChannel.js'

serverConfig.channel = { transports: ['sse'] }

let server: Server
let telefuncUrl: string
beforeAll(async () => {
  const telefunc = new Telefunc()
  server = createServer((req, res) => void telefunc.serve({ req, res }))
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  telefuncUrl = `http://127.0.0.1:${address.port}/_telefunc`
})
afterAll(() => {
  server.closeAllConnections()
  server.close()
})

test("over SSE in batch POSTs, a join whose RECONCILE's POST is never answered while the event stream stays live reconnects at that RECONCILE's deadline, and the joined channel opens within the server's TTL", async () => {
  // A browser on HTTP/1.1 can't stream a request body: its uplink is batch POSTs.
  const realFetch = globalThis.fetch
  const fetchSpy = vi
    .spyOn(globalThis, 'fetch')
    .mockImplementation((input, init) =>
      init !== undefined && 'duplex' in init && init.duplex === 'half'
        ? Promise.reject(new TypeError('Failed to fetch'))
        : realFetch(input, init),
    )
  // The first wire's second RECONCILE, the join's, is held where it arrives, so its POST is never answered.
  const dispatch = ChannelMux.prototype.dispatchWireMessage
  const reconciles = new Map<unknown, number>()
  let held = 0
  const dispatchSpy = vi.spyOn(ChannelMux.prototype, 'dispatchWireMessage').mockImplementation(function (
    this: ChannelMux,
    wire,
    raw,
  ) {
    if (decode(raw).tag === TAG.RECONCILE) {
      const seen = (reconciles.get(wire) ?? 0) + 1
      reconciles.set(wire, seen)
      if (seen === 2 && reconciles.size === 1) {
        held++
        return new Promise<null>(() => {})
      }
    }
    return dispatch.call(this, wire, raw)
  })
  const connection = { telefuncUrl, transports: ['sse' as const], connectionKey: crypto.randomUUID() }
  const lead = new ServerChannel<string, string>()
  getChannelMux().registerChannel(lead, ATTACHABLE_NOW)
  const neighbour = new ClientChannel<string, string>({ channelId: lead.id, connection })
  try {
    await Promise.all([lead.opened, neighbour.opened])
    const joined = new ServerChannel<string, string>()
    getChannelMux().registerChannel(joined, ATTACHABLE_NOW)
    const joinedAt = performance.now()
    const joining = new ClientChannel<string, string>({ channelId: joined.id, connection })
    try {
      await Promise.all([joined.opened, joining.opened])
      expect(held).toBe(1)
      expect(performance.now() - joinedAt).toBeLessThan(CHANNEL_CONNECT_TTL_MS)
      const got = new Promise((resolve) => joined.listen(resolve))
      void joining.send('after')
      expect(await got).toBe('after')
    } finally {
      joining.abort()
    }
  } finally {
    neighbour.abort()
    dispatchSpy.mockRestore()
    fetchSpy.mockRestore()
  }
}, 30_000)
