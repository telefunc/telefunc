// What a close keeps for its page, its last frames, goes once the page says it has them: the RECONCILE that declares the
// next channel does, and in a close the page decides, its verdict. Over a real server and page, with no PING due for a
// minute.

import { afterAll, beforeAll, describe, expect, test, vi } from 'vitest'
import { createServer, type Server } from 'node:http'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { ServerChannel } from './server/ServerChannel.js'
import { ClientChannel } from './client/ClientChannel.js'
import { WireLifecycle } from './client/WireLifecycle.js'
import { fakeMuxChannel } from './client/fakeMuxChannel.fixture.js'
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
  for (const decider of ['server', 'page', 'crossed', 'answering'] as const) {
    const close = {
      server: 'a close the server decided',
      page: 'a close the page decided',
      crossed: 'a crossed close, a message before it,',
      answering: "a close the server decided while it answered the page's acknowledged send",
    }[decider]
    describe(`over ${transport === 'sse' ? 'SSE' : 'a WebSocket'}, right after ${close} with no frame from the page since`, () => {
      for (const [text, sent, expected] of [
        [FITS, 'maxMemory to the byte', 'answered'],
        [`${FITS}x`, 'one byte more', 'ChannelOverflowError'],
      ] as const) {
        test(`a send of ${sent} on another channel of its connection is ${expected === 'answered' ? 'answered' : 'refused with ChannelOverflowError'}`, async () => {
          const [first, firstPage] = pair(transport)
          const [other, otherPage] = pair(transport)
          await Promise.all([first.opened, firstPage.opened, other.opened, otherPage.opened])
          otherPage.listen(() => 'answered')
          if (decider === 'crossed') {
            // Each end's CLOSE leaves before the other's arrives; the message before it is one the page never confirms.
            void first.send('before', { ack: false })
            expect(await Promise.all([first.close(), firstPage.close()])).toEqual([null, null])
          } else if (decider === 'server') {
            await first.close()
            await firstPage.closed
          } else if (decider === 'answering') {
            // Its answer leaves after the server's CLOSE: the page reads it before it confirms the close.
            let answer = (_: string) => {}
            const asked = new Promise<void>((resolve) =>
              first.listen(() => {
                resolve()
                return new Promise<string>((answered) => (answer = answered))
              }),
            )
            const answered = firstPage.send('ask', { ack: true })
            await asked
            const closing = first.close()
            answer('answer')
            expect(await answered).toBe('answer')
            expect(await closing).toBe(null)
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

          const verdict = decode(encode.closeDone(0, first._replayBuffer?.seq ?? 0, first._lastClientSeq))
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

// The page lets go of what it sent once the server says it read it: in a close, its CLOSE_ACK says so.
for (const transport of ['sse', 'ws'] as const) {
  test(`over ${transport === 'sse' ? 'SSE' : 'a WebSocket'}, what the page sent before its close leaves its replay when the server's CLOSE_ACK arrives, while the server's cleanup still runs`, async () => {
    const channel = new ServerChannel()
    getChannelMux().registerChannel(channel)
    channel.listenBinary(() => {})
    let cleanedUp = () => {}
    channel.onClose(() => new Promise<void>((resolve) => (cleanedUp = resolve)))
    // The page's end of the channel, as the page's connection drives it: what the server sends it is read, by tag.
    const read: number[] = []
    const page = fakeMuxChannel({ id: channel.id, _dispatchFrame: (frame) => void read.push(frame.tag) })
    const lifecycle = new WireLifecycle({
      owner: { onDisposed() {} },
      telefuncUrl,
      options: { transports: [transport], fetchImpl: fetch, idleTimeout: 0 },
    })
    lifecycle.register(page)
    const record = lifecycle.channels.of(page)
    if (record === undefined) throw new Error('not registered')
    const send = (frame: (seq: number) => Uint8Array<ArrayBuffer>) => {
      const seq = record.replay.nextSeq()
      lifecycle.gate.send(record, seq, frame(seq), 'control')
    }
    await channel.opened
    // Less than a quarter of its credit window, which the server would grant again in a WINDOW that says it read it.
    send((seq) => encode.binary(record.ix, new Uint8Array(100_000), seq))
    send((seq) => encode.close(record.ix, 5_000, seq))
    await vi.waitFor(() => expect(read).toContain(TAG.CLOSE_ACK))
    expect(record.replay.length).toBe(0)
    cleanedUp()
    await vi.waitFor(() => expect(read).toContain(TAG.CLOSE_DONE))
    send((seq) => encode.closeDone(record.ix, record.lastSeq, seq))
    expect(await channel.closed).toBe(null)
    lifecycle.unregister(page)
  })
}
