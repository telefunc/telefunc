import { expect, test } from 'vitest'
import http from 'node:http'
import net from 'node:net'
import { Telefunc } from './node.js'

test("a WebSocket upgrade whose path isn't a URL, such as //, leaves the server up", async () => {
  const server = http.createServer()
  new Telefunc().installWebSocket(server)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const { port } = server.address() as { port: number }
  const socket = net.connect(port, '127.0.0.1')
  await new Promise<void>((resolve) => socket.once('connect', resolve))
  socket.write(
    'GET // HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n' +
      'Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n',
  )
  await new Promise((resolve) => setTimeout(resolve, 100))
  expect(server.listening).toBe(true)
  socket.destroy()
  server.closeAllConnections()
  server.close()
})
