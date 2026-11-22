// A browser can attach to a channel a call returned only once it has sent the call's whole body, so the channel's
// connect window counts from the end of that upload, not from the answer. The row sends a call that returns a Channel and
// paces its File's upload past `config.channel.connectTtl` (1 s here); once the upload ends, a page attaches over SSE.
// `config.channel` can't change once a channel used it, so the row has a file of its own.

import { afterAll, beforeAll, expect, test } from 'vitest'
import { createServer, type Server } from 'node:http'
import { connect } from 'node:net'
import { once } from 'node:events'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { Telefunc } from './node.js'
import { config } from '../node/server/serverConfig.js'
import { WIRE_VERSION } from '../wire-protocol/constants.js'
import { Channel } from '../wire-protocol/channel/server/ServerChannel.js'
import { ClientChannel } from '../wire-protocol/channel/client/ClientChannel.js'

const file = '/requestBody.telefunc.js'
config.root = dirname(fileURLToPath(import.meta.url))
config.telefuncFiles = [`${config.root}${file}`]
config.disableNamingConvention = true
const CONNECT_TTL_MS = 1000
config.channel = { transports: ['sse'], connectTtl: CONNECT_TTL_MS }

const encoder = new TextEncoder()
const telefuncPath = `/_telefunc?wire=${WIRE_VERSION}`
let server: Server
let port = 0

beforeAll(async () => {
  const telefunc = new Telefunc()
  server = createServer((req, res) => void telefunc.serve({ req, res }).catch(() => {}))
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  port = address.port
})
afterAll(() => {
  server.closeAllConnections()
  server.close()
})

test('a call that returns a Channel and leaves its File unread, whose upload takes longer than the connect window: a page that attaches once the upload has ended opens the channel (Telefunc given a Node request)', async () => {
  const made: Channel[] = []
  const onChannel = (holder: { channel?: Channel }) => {
    holder.channel = new Channel()
    made.push(holder.channel)
  }
  process.on('requestBody.telefunc:channel', onChannel)
  const size = 16 * 64 * 1024
  const metadata = encoder.encode(
    JSON.stringify({
      file,
      name: 'returnsChannelIgnoringBlob',
      args: [`!TelefuncBlob:${JSON.stringify({ index: 0, size, type: '' })}`],
    }),
  )
  const start = new Uint8Array(4 + metadata.length)
  new DataView(start.buffer).setUint32(0, metadata.length)
  start.set(metadata, 4)
  const socket = connect(port, '127.0.0.1')
  socket.on('error', () => {})
  socket.resume()
  await once(socket, 'connect')
  socket.write(
    `POST ${telefuncPath} HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nx-telefunc-request: bin\r\nContent-Type: application/octet-stream\r\nContent-Length: ${start.length + size}\r\n\r\n`,
  )
  socket.write(start)
  // The File in 16 parts, 100 ms apart: its upload ends about 1.6 s after the answer, past the connect window.
  for (let part = 0; part < 16; part++) {
    await new Promise((resolve) => setTimeout(resolve, 100))
    socket.write(new Uint8Array(64 * 1024).fill(120))
  }
  const channel = made[0]
  if (!channel) throw new Error('no channel')
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    connection: {
      telefuncUrl: `http://127.0.0.1:${port}/_telefunc`,
      transports: ['sse'],
      idleTimeout: 0,
      connectionKey: crypto.randomUUID(),
    },
  })
  const opened = await page.opened.then(
    () => 'opened',
    (err: Error) => err.message,
  )
  process.off('requestBody.telefunc:channel', onChannel)
  page.abort()
  channel.abort()
  socket.destroy()
  expect(opened).toBe('opened')
})
