// Pages on this machine, each on its own WebSocket, subscribed to a broadcast of 8 MiB a second: what each one gets. Over a
// real server and pages, which say what they took mostly when they send something, and these send nothing.

import { afterAll, beforeAll, expect, test, vi } from 'vitest'
import { createServer, type Server } from 'node:http'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { Broadcast, ServerBroadcast } from './server/ServerBroadcast.js'
import { ClientBroadcast } from './client/ClientBroadcast.js'
import { getChannelMux } from './server/ChannelMux.js'
import { ATTACHABLE_NOW } from './server/ServerChannel.js'

serverConfig.channel = { transports: ['ws'] }

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
  server.closeAllConnections()
  server.close()
})

const PAGES = 20
const PUBLISHES = 12

test(`${PAGES} pages subscribed to 8 MiB a second each get every publish, in order`, async () => {
  const key = `feed:${crypto.randomUUID()}`
  const pages = Array.from({ length: PAGES }, () => {
    const channel = new ServerBroadcast({ key })
    getChannelMux().registerChannel(channel, ATTACHABLE_NOW)
    const page = new ClientBroadcast<string>({
      channelId: channel.id,
      connection: { telefuncUrl, transports: ['ws'], connectionKey: crypto.randomUUID() },
    })
    const seqs: number[] = []
    let large = 0
    page.subscribeBinary((data, info) => {
      seqs.push(info.seq)
      if (data.byteLength > 1) large++
    })
    return { channel, page, seqs, large: () => large }
  })
  try {
    await Promise.all(pages.flatMap(({ channel, page }) => [channel.opened, page.opened]))
    // Every page's subscription is in effect once each reads a publish.
    await vi.waitFor(async () => {
      await Broadcast.publishBinary(key, new Uint8Array(1))
      for (const { seqs } of pages) expect(seqs.length).toBeGreaterThan(0)
    })
    const message = new Uint8Array(8 * 1024 * 1024)
    for (let n = 0; n < PUBLISHES; n++) {
      await Broadcast.publishBinary(key, message)
      await new Promise((resolve) => setTimeout(resolve, 1_000))
    }
    await vi.waitFor(() => {
      expect(pages.map(({ large }) => large())).toEqual(pages.map(() => PUBLISHES))
    }, 30_000)
    for (const { seqs } of pages) {
      const [first = 0] = seqs
      expect(seqs).toEqual(seqs.map((_, i) => first + i))
    }
  } finally {
    for (const { page } of pages) page.abort()
  }
}, 120_000)
