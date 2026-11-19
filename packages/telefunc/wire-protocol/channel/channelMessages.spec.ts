import { afterAll, beforeAll, describe, expect, test } from 'vitest'
import { createServer, type Server } from 'node:http'
import { setFlagsFromString } from 'node:v8'
import { runInNewContext } from 'node:vm'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { ServerChannel } from './server/ServerChannel.js'
import { ClientChannel } from './client/ClientChannel.js'
import { getChannelMux } from './server/ChannelMux.js'

serverConfig.channel = { transports: ['sse', 'ws'], pingInterval: 60_000 }

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

/** A server channel and the page's end of it, over `transport`, both open. */
async function pair(transport: 'sse' | 'ws') {
  const channel = new ServerChannel<string, string>({ ack: true })
  getChannelMux().registerChannel(channel)
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    ack: true,
    connection: { telefuncUrl, transports: [transport], idleTimeout: 0 },
  })
  await Promise.all([channel.opened, page.opened])
  return [channel, page] as const
}

// A collection the test can force, as `node --expose-gc` gives.
setFlagsFromString('--expose-gc')
const gc: unknown = runInNewContext('gc')
/** Collects what nothing reaches any more, across a few turns. */
async function collect() {
  if (typeof gc !== 'function') throw new Error('no gc')
  for (let i = 0; i < 8; i++) {
    await new Promise((resolve) => setTimeout(resolve, 0))
    gc()
  }
}

const within = <T>(promise: Promise<T>, ms: number) =>
  Promise.race([promise, new Promise<'still pending'>((resolve) => setTimeout(() => resolve('still pending'), ms))])

for (const transport of ['sse', 'ws'] as const) {
  for (const receiver of ['server', 'page'] as const) {
    describe(`over ${transport === 'sse' ? 'SSE' : 'a WebSocket'}, the ${receiver}'s messages() loop`, () => {
      test('aborted while it holds an acknowledged message lets it go: the next listener answers it', async () => {
        const [channel, page] = await pair(transport)
        const [sender, reader] = receiver === 'server' ? [page, channel] : [channel, page]
        const controller = new AbortController()
        const messages = reader.messages({ signal: controller.signal })
        const first = messages.next()
        reader.listen(() => 'from the next listener')
        const reply = sender.send('held', { ack: true })

        expect((await first).value).toEqual({ type: 'text', data: 'held' })
        controller.abort(new Error('the app stops reading'))

        expect(await within(reply, 2_000)).toBe('from the next listener')
        await expect(messages.next()).rejects.toThrow('the app stops reading')
        await sender.close()
      })

      for (const [kind, end] of [
        ['text', 'aborted'],
        ['binary', 'aborted'],
        ['text', 'returned'],
      ] as const) {
        test(`${end} while the app still holds it, reaches no ${kind} message it yielded or queued`, async () => {
          const channel = new ServerChannel<{ i: number }, { i: number }>({ ack: true })
          getChannelMux().registerChannel(channel)
          const page = new ClientChannel<{ i: number }, { i: number }>({
            channelId: channel.id,
            ack: true,
            connection: { telefuncUrl, transports: [transport], idleTimeout: 0 },
          })
          await Promise.all([channel.opened, page.opened])
          const [sender, reader] = receiver === 'server' ? [page, channel] : [channel, page]
          const send = (i: number) => (kind === 'text' ? sender.send({ i }) : sender.sendBinary(new Uint8Array([i])))
          // Each message as the loop's listeners get it, held only weakly here.
          const received: WeakRef<object>[] = []
          const listen = reader.listen
          const listenBinary = reader.listenBinary
          reader.listen = (callback, options) =>
            listen.call(reader, (data) => (received.push(new WeakRef(data)), callback(data)), options)
          reader.listenBinary = (callback, options) =>
            listenBinary.call(reader, (data) => (received.push(new WeakRef(data)), callback(data)), options)
          const controller = new AbortController()
          const messages = reader.messages({ signal: controller.signal })
          // The loop yields the first, and queues the next two.
          await (async () => {
            // Its first step registers its listeners.
            const first = messages.next()
            reader.listen = listen
            reader.listenBinary = listenBinary
            void send(1)
            expect((await first).done).toBe(false)
            void send(2)
            void send(3)
            for (let i = 0; i < 100 && received.length < 3; i++) await new Promise((resolve) => setTimeout(resolve, 10))
          })()
          expect(received.length).toBe(3)
          if (end === 'aborted') controller.abort(new Error('the app stops reading'))
          else {
            expect(messages.return).toBeDefined()
            await messages.return?.(undefined)
          }
          await collect()
          // The app keeps the iterator, as one that tears it down later does.
          expect(typeof messages.next).toBe('function')
          expect(received.map((ref) => ref.deref() !== undefined)).toEqual([false, false, false])
          await channel.close({ timeout: 100 })
        })
      }
    })
  }
}
