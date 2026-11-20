// What a close keeps for its page, its last frames, goes once the page says it has them: the RECONCILE that declares the
// next channel does, and in a close the page decides, its verdict. Over a real server and page, with no PING due for a
// minute.

import { afterAll, beforeAll, describe, expect, test } from 'vitest'
import { createServer, type Server } from 'node:http'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { ServerChannel } from './server/ServerChannel.js'
import { ClientChannel } from './client/ClientChannel.js'
import { getChannelMux } from './server/ChannelMux.js'
import { decode, encode } from '../wire/frame.js'
import { TAG } from '../wire/tags.js'

serverConfig.channel = { transports: ['sse', 'ws'], pingInterval: 60_000, maxMemory: '2m' }

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

/** A server channel and the page's end of it, over `transport`, on the page's one connection for it. */
function pair(transport: 'sse' | 'ws') {
  const channel = new ServerChannel<string, string>({ ack: true })
  getChannelMux().registerChannel(channel)
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    ack: true,
    // Up for the next channel: the RECONCILE that declares it names the first one closed.
    connection: { telefuncUrl, transports: [transport], idleTimeout: 60_000 },
  })
  return [channel, page] as const
}

// 2 MiB is these 2,096,631 UTF-8 bytes, the two quotes around them, a frame's 7 bytes and a held send's 512.
const FITS = `${'é'.repeat(1_048_315)}x`

for (const transport of ['sse', 'ws'] as const) {
  describe(`over ${transport === 'sse' ? 'SSE' : 'a WebSocket'}, once the page has the verdict of a close the server decided`, () => {
    for (const [text, sent, expected] of [
      [FITS, 'maxMemory to the byte', 'answered'],
      [`${FITS}x`, 'one byte more', 'ChannelOverflowError'],
    ] as const) {
      test(`the next channel's send of ${sent} is ${expected === 'answered' ? 'answered' : 'refused with ChannelOverflowError'}`, async () => {
        const [first, firstPage] = pair(transport)
        await Promise.all([first.opened, firstPage.opened])
        await first.close()
        await firstPage.closed

        const [next, nextPage] = pair(transport)
        nextPage.listen(() => 'answered')
        const outcome = new Promise<string>((resolve) =>
          next.onOpen(
            () =>
              void next
                .send(text, { ack: true })
                .then(String, (err: Error) => err.name)
                .then(resolve),
          ),
        )
        // Left open: its send settled, so it holds nothing, where a close would until its page said so.
        expect(await outcome).toBe(expected)
      })
    }
  })
}

for (const transport of ['sse', 'ws'] as const) {
  for (const decider of ['server', 'page'] as const) {
    describe(`over ${transport === 'sse' ? 'SSE' : 'a WebSocket'}, right after a close the ${decider} decided, with no frame from the page since`, () => {
      for (const [text, sent, expected] of [
        [FITS, 'maxMemory to the byte', 'answered'],
        [`${FITS}x`, 'one byte more', 'ChannelOverflowError'],
      ] as const) {
        test(`a send of ${sent} on another channel of its connection is ${expected === 'answered' ? 'answered' : 'refused with ChannelOverflowError'}`, async () => {
          const [first, firstPage] = pair(transport)
          const [other, otherPage] = pair(transport)
          await Promise.all([first.opened, firstPage.opened, other.opened, otherPage.opened])
          otherPage.listen(() => 'answered')
          if (decider === 'server') {
            await first.close()
            await firstPage.closed
          } else {
            await firstPage.close()
            await first.closed
          }

          expect(await other.send(text, { ack: true }).then(String, (err: Error) => err.name)).toBe(expected)
          // The RECONCILE that declares one more lets go of what the close kept, so the next row has all of `maxMemory`.
          const [last, lastPage] = pair(transport)
          await Promise.all([last.opened, lastPage.opened])
        })
      }

      if (decider === 'page')
        test('its verdict again, as a replay after a reconnect would bring it, changes nothing: the close stays clean, and a send of maxMemory to the byte on another channel is answered', async () => {
          const [first, firstPage] = pair(transport)
          const [other, otherPage] = pair(transport)
          await Promise.all([first.opened, firstPage.opened, other.opened, otherPage.opened])
          otherPage.listen(() => 'answered')
          await firstPage.close()
          await first.closed

          const verdict = decode(encode.closeDone(0, first._lastClientSeq))
          if (verdict.tag !== TAG.CLOSE_DONE) throw new Error('Expected CLOSE_DONE frame')
          first._dispatchFrame(verdict)
          expect(await first.closed).toBe(null)
          expect(await other.send(FITS, { ack: true }).then(String, (err: Error) => err.name)).toBe('answered')
          const [last, lastPage] = pair(transport)
          await Promise.all([last.opened, lastPage.opened])
        })
    })
  }
}
