import { afterAll, beforeAll, describe, expect, test } from 'vitest'
import { createServer, type Server } from 'node:http'
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
    })
  }
}
