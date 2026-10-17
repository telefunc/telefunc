import { afterEach, expect, test, vi } from 'vitest'
import { Telefunc } from './deno.js'
import { getServerConfig } from '../node/server/serverConfig.js'

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
})

test("a WebSocket client that goes silent is closed at its ping deadline, though Deno's WebSocket has no terminate()", async () => {
  vi.useFakeTimers()
  // Deno's WebSocket: close() but no terminate().
  const socket = Object.assign(new EventTarget(), {
    closed: false,
    send() {},
    close: () => void (socket.closed = true),
  })
  vi.stubGlobal('Deno', { upgradeWebSocket: () => ({ socket, response: new Response(null) }) })
  const telefunc = new Telefunc()
  const config = getServerConfig()
  const request = new Request(`http://localhost${config.telefuncUrl}`, { headers: { upgrade: 'websocket' } })
  await telefunc.serve({ request, info: {} as never })
  socket.dispatchEvent(new Event('open'))
  await vi.advanceTimersByTimeAsync(2 * config.channel.pingInterval + 1)
  expect(socket.closed).toBe(true)
})
