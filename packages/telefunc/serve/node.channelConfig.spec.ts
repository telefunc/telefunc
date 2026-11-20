import { expect, onTestFinished, test } from 'vitest'
import { createServer } from 'node:http'
import { once } from 'node:events'
import { Telefunc } from './node.js'
import { config } from '../node/server/serverConfig.js'
import { WIRE_VERSION } from '../wire-protocol/constants.js'

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
