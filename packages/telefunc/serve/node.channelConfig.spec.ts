import { expect, onTestFinished, test, vi } from 'vitest'
import { createServer, request } from 'node:http'
import { once } from 'node:events'
import { randomBytes } from 'node:crypto'
import { Telefunc } from './node.js'
import { config } from '../node/server/serverConfig.js'
import { WIRE_VERSION } from '../wire-protocol/constants.js'

// First: a page's WebSocket that opens locks `config.channel` for the rest of the file.
test("a refused WebSocket upgrade from another site's page leaves config.channel assignable", async () => {
  const server = createServer()
  new Telefunc().installWebSocket(server)
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  onTestFinished(() => {
    server.close()
  })
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
  onTestFinished(() => warn.mockRestore())
  const req = request({
    host: '127.0.0.1',
    port: address.port,
    path: `/_telefunc?wire=${WIRE_VERSION}`,
    headers: {
      Connection: 'Upgrade',
      Upgrade: 'websocket',
      'Sec-WebSocket-Version': '13',
      'Sec-WebSocket-Key': randomBytes(16).toString('base64'),
      Origin: 'https://another-site.example',
    },
  })
  const status = new Promise<number | undefined>((resolve) => {
    req.on('response', (response) => {
      response.resume()
      resolve(response.statusCode)
    })
  })
  req.end()
  expect(await status).toBe(403)
  config.channel = { pingInterval: 2000 }
  onTestFinished(() => {
    config.channel = {}
  })
  expect(config.channel.pingInterval).toBe(2000)
})

test("config.channel refuses a change once a page's WebSocket has opened, on a server that has made no channel", async () => {
  const server = createServer()
  new Telefunc().installWebSocket(server)
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  onTestFinished(() => {
    server.close()
  })
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  const ws = new WebSocket(`ws://127.0.0.1:${address.port}/_telefunc?wire=${WIRE_VERSION}`)
  onTestFinished(() => ws.close())
  await once(ws, 'open')
  expect(() => {
    config.channel = { reconnectTimeout: 123_456 }
  }).toThrow(
    "`config.channel` can't change once the server has used it, from its first channel or page connection on, as those already open would keep the old values",
  )
})

// After the test above, where a page's WebSocket opened.
test('config.channel refuses a change of its allowed origins once a page has connected', () => {
  expect(() => {
    config.channel.allowedOrigins = ['https://app.example.com']
  }).toThrow("`config.channel` can't change once the server has used it")
})
