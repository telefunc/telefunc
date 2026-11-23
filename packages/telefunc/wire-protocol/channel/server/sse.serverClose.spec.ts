// A server that closes its connections (`server.closeAllConnections()`, as a shutdown does) while a page is connected over
// SSE: Node ends that page's request under the server's read of its body, with its own `aborted` error. The server
// ended it, not the app, so nothing is printed and nothing is left uncaught. Over a real Node server and page.

import { expect, onTestFinished, test, vi } from 'vitest'
import { createServer } from 'node:http'
import { once } from 'node:events'
import { Telefunc } from '../../../serve/node.js'
import { config as serverConfig } from '../../../node/server/serverConfig.js'
import { ServerChannel } from './ServerChannel.js'
import { ClientChannel } from '../client/ClientChannel.js'
import { getChannelMux } from './ChannelMux.js'

serverConfig.channel = { transports: ['sse'] }

const pause = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

test('a server that closes its connections while a page is connected over SSE prints nothing, and leaves nothing uncaught', async () => {
  const printed: string[] = []
  const print = (...args: unknown[]) => void printed.push(args.map(String).join(' '))
  for (const method of ['error', 'warn', 'log', 'info'] as const) vi.spyOn(console, method).mockImplementation(print)
  const uncaught: string[] = []
  const onUncaught = (err: unknown) => void uncaught.push(String(err))
  process.on('uncaughtException', onUncaught)
  process.on('unhandledRejection', onUncaught)
  onTestFinished(() => {
    vi.restoreAllMocks()
    process.off('uncaughtException', onUncaught)
    process.off('unhandledRejection', onUncaught)
  })

  const telefunc = new Telefunc()
  const server = createServer((req, res) => void telefunc.serve({ req, res }))
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  const channel = new ServerChannel<string, string>()
  getChannelMux().registerChannel(channel)
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    connection: { telefuncUrl: `http://127.0.0.1:${address.port}/_telefunc`, transports: ['sse'] },
  })
  await Promise.all([channel.opened, page.opened])

  server.closeAllConnections()
  server.close()
  await once(server, 'close')
  page.abort()
  channel.abort()
  await pause(200)
  expect({ printed, uncaught }).toEqual({ printed: [], uncaught: [] })
})
