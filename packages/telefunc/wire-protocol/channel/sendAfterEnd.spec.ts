// A callback that sends on its channel after the channel ended raced the end, as one whose awaited send rejects with it:
// the send throws `ChannelClosedError` and nothing is reported. Over a real server and page.

import { afterAll, beforeAll, describe, expect, test, vi } from 'vitest'
import { createServer, type Server } from 'node:http'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { onBug } from '../../node/server/runTelefunc/onBug.js'
import { Abort } from '../../shared/Abort.js'
import { ChannelClosedError, ChannelPageLeftError } from './channelErrors.js'
import { ServerChannel } from './server/ServerChannel.js'
import { Broadcast, ServerBroadcast } from './server/ServerBroadcast.js'
import { ClientChannel } from './client/ClientChannel.js'
import { ClientBroadcast } from './client/ClientBroadcast.js'
import { getChannelMux } from './server/ChannelMux.js'

// The page's transport listens for `pagehide` on the global object, as in a browser.
const pageEvents = new EventTarget()
Object.assign(globalThis, {
  addEventListener: pageEvents.addEventListener.bind(pageEvents),
  removeEventListener: pageEvents.removeEventListener.bind(pageEvents),
})
/** The page unloads: it is closed, reloaded or navigated away from. */
const leavePage = () => pageEvents.dispatchEvent(Object.assign(new Event('pagehide'), { persisted: false }))

serverConfig.channel = { transports: ['ws'], pingInterval: 60_000 }
serverConfig.disableNamingConvention = true

let server: Server
let telefuncUrl: string
const serverBugs: unknown[] = []

beforeAll(async () => {
  const telefunc = new Telefunc()
  server = createServer((req, res) => void telefunc.serve({ req, res }))
  telefunc.installWebSocket(server)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  telefuncUrl = `http://127.0.0.1:${address.port}/_telefunc`
  onBug((err) => serverBugs.push(err))
})
afterAll(() => {
  server.closeAllConnections()
  server.close()
})

/** A server channel and the page's end of it, both open. */
async function pair() {
  const channel = new ServerChannel<unknown, unknown>()
  getChannelMux().registerChannel(channel)
  const page = new ClientChannel<unknown, unknown>({
    channelId: channel.id,
    connection: { telefuncUrl, transports: ['ws'], idleTimeout: 0 },
  })
  await Promise.all([channel.opened, page.opened])
  return [channel, page] as const
}

/** What the server's `onBug()` gets and what the page prints as a channel error, while `run` runs. */
async function reported(run: () => Promise<void>) {
  const page: unknown[] = []
  const printed = vi.spyOn(console, 'error').mockImplementation((...args: unknown[]) => {
    if (args[0] === '[telefunc:channel-error]') page.push(args[1])
  })
  serverBugs.length = 0
  try {
    await run()
  } finally {
    printed.mockRestore()
  }
  return { server: serverBugs, page }
}

type End = ServerChannel<unknown, unknown> | ClientChannel<unknown, unknown>
const SENDS = {
  'send()': (end: End) => end.send('late'),
  'sendBinary()': (end: End) => end.sendBinary(new Uint8Array([1])),
}

describe('a listen() callback that sends after its channel ended', () => {
  for (const [method, send] of Object.entries(SENDS)) {
    test(`on the server, its page left: ${method} throws one ChannelClosedError whose cause is the end, and nothing is reported`, async () => {
      const [channel, page] = await pair()
      const thrown: unknown[] = []
      const { server, page: printed } = await reported(async () => {
        let started = false
        const listened = new Promise<void>((resolve) => {
          channel.listen(async () => {
            started = true
            await channel.closed
            for (let i = 0; i < 2; i++) {
              try {
                await send(channel)
              } catch (err) {
                thrown.push(err)
              }
            }
            resolve()
            await send(channel)
          })
        })
        void page.send('go')
        await vi.waitFor(() => expect(started).toBe(true))
        leavePage()
        await listened
        await new Promise((resolve) => setTimeout(resolve, 50))
      })

      const end = await channel.closed
      expect(end).toBeInstanceOf(ChannelPageLeftError)
      expect(thrown).toHaveLength(2)
      expect(thrown[0]).toBeInstanceOf(ChannelClosedError)
      expect(thrown[1]).toBe(thrown[0])
      expect(thrown[0] instanceof Error && thrown[0].cause).toBe(end)
      expect({ server, page: printed }).toEqual({ server: [], page: [] })
    })

    test(`on the page, the server aborted: ${method} throws one ChannelClosedError whose cause is the end, and nothing is printed`, async () => {
      const [channel, page] = await pair()
      const thrown: unknown[] = []
      const { server, page: printed } = await reported(async () => {
        let started = false
        const listened = new Promise<void>((resolve) => {
          page.listen(async () => {
            started = true
            await page.closed
            for (let i = 0; i < 2; i++) {
              try {
                await send(page)
              } catch (err) {
                thrown.push(err)
              }
            }
            resolve()
            await send(page)
          })
        })
        void channel.send('go')
        await vi.waitFor(() => expect(started).toBe(true))
        channel.abort('stop')
        await listened
        await new Promise((resolve) => setTimeout(resolve, 50))
      })

      const end = await page.closed
      expect(end).toBeInstanceOf(Abort)
      expect(thrown).toHaveLength(2)
      expect(thrown[0]).toBeInstanceOf(ChannelClosedError)
      expect(thrown[1]).toBe(thrown[0])
      expect(thrown[0] instanceof Error && thrown[0].cause).toBe(end)
      expect({ server, page: printed }).toEqual({ server: [], page: [] })
    })
  }

  test("what another channel's send throws after that channel ended is reported", async () => {
    const [channel, page] = await pair()
    const [other, otherPage] = await pair()
    const { server } = await reported(async () => {
      await otherPage.close({ timeout: 1_000 })
      const listened = new Promise<void>((resolve) => {
        channel.listen(async () => {
          resolve()
          await other.send('to the other channel')
        })
      })
      void page.send('go')
      await listened
      await new Promise((resolve) => setTimeout(resolve, 50))
    })
    expect(server).toEqual([expect.any(ChannelClosedError)])
  })
})

const PUBLISHES = {
  'publish()': (page: ClientBroadcast<string>) => page.publish('late'),
  'publishBinary()': (page: ClientBroadcast<string>) => page.publishBinary(new Uint8Array([1])),
}

describe("a page's subscribe() callback that publishes after its broadcast ended", () => {
  for (const [method, publish] of Object.entries(PUBLISHES)) {
    test(`the server aborted: ${method} throws one ChannelClosedError whose cause is the end, and nothing is printed`, async () => {
      const key = crypto.randomUUID()
      const channel = new ServerBroadcast<string>({ key })
      getChannelMux().registerChannel(channel)
      const page = new ClientBroadcast<string>({
        channelId: channel.id,
        connection: { telefuncUrl, transports: ['ws'], idleTimeout: 0 },
      })
      await Promise.all([channel.opened, page.opened])
      const thrown: unknown[] = []
      const { page: printed } = await reported(async () => {
        let started = false
        let published = () => {}
        const publishedAll = new Promise<void>((resolve) => (published = resolve))
        const subscription = page.subscribe(async () => {
          started = true
          await page.closed
          for (let i = 0; i < 2; i++) {
            try {
              await publish(page)
            } catch (err) {
              thrown.push(err)
            }
          }
          published()
          await publish(page)
        })
        await subscription.opened
        await Broadcast.publish(key, 'go')
        await vi.waitFor(() => expect(started).toBe(true))
        channel.abort('stop')
        await publishedAll
        await new Promise((resolve) => setTimeout(resolve, 50))
      })

      const end = await page.closed
      expect(end).toBeInstanceOf(Abort)
      expect(thrown).toHaveLength(2)
      expect(thrown[0]).toBeInstanceOf(ChannelClosedError)
      expect(thrown[1]).toBe(thrown[0])
      expect(thrown[0] instanceof Error && thrown[0].cause).toBe(end)
      expect(printed).toEqual([])
    })
  }
})

test('an onClose() callback that sends on its own channel after the end is reported: the send can never go', async () => {
  const [channel] = await pair()
  const { server } = await reported(async () => {
    const called = new Promise<void>((resolve) => {
      channel.onClose(() => {
        resolve()
        void channel.send('after the end')
      })
    })
    channel.abort('stop')
    await called
  })
  expect(server).toEqual([expect.any(ChannelClosedError)])
})

test('a callback that rejects with no value once its channel closed gracefully is reported', async () => {
  const [channel, page] = await pair()
  let resume = () => {}
  const resumed = new Promise<void>((resolve) => (resume = resolve))
  const { server } = await reported(async () => {
    channel.onOpen(async () => {
      await resumed
      return Promise.reject()
    })
    await page.close({ timeout: 1_000 })
    await channel.closed
    resume()
    await new Promise((resolve) => setTimeout(resolve, 50))
  })
  expect(await channel.closed).toBeNull()
  expect(server).toEqual([new Error('undefined')])
})
