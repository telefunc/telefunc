// An SSE page's WebSocket probe, against a real server that refuses the upgrade: the probe fails as the refusal comes,
// on Node 22's WebSocket too, which fires `error` and no `close` for it.

import { afterAll, beforeAll, expect, test } from 'vitest'
import { createServer, type Server } from 'node:http'
import { WsTransport } from './WsTransport.js'
import type { TransportOwner } from './WireLifecycle.js'
import { WS_PROBE_TIMEOUT_MS } from '../../constants.js'

let server: Server
let telefuncUrl: string
beforeAll(async () => {
  server = createServer((_req, res) => res.writeHead(404).end())
  server.on('upgrade', (_req, socket) => {
    socket.once('finish', socket.destroy)
    socket.end('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n')
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  telefuncUrl = `http://127.0.0.1:${address.port}/_telefunc`
})
afterAll(() => {
  server.closeAllConnections()
  server.close()
})

const unreached = () => {
  throw new Error('a probe never asks its transport for this')
}
const owner: TransportOwner = {
  onTransportOpen: unreached,
  onTransportAnswered: unreached,
  onTransportClosed: unreached,
  onTransportRefused: unreached,
  onTransportFrame: unreached,
  onTransportPong: unreached,
  onTransportReconciledPiece: unreached,
  onTransportHeadroom: unreached,
  onTransportBatched: unreached,
  onTransportReconcileSent: unreached,
  ssePostLimit: Infinity,
  stalledChannels: new Set(),
  onStalledPostLost: unreached,
  stillOwed: unreached,
  onTransportTooLarge: unreached,
  stageInitialReconcileBatch: unreached,
}

test('a probe whose upgrade the server refuses fails at the refusal, well before its timeout, and tells its owner nothing', async () => {
  const transport = new WsTransport({ telefuncUrl, owner })
  const told: string[] = []
  const started = performance.now()
  const probed = await transport.probe({
    onProbePong: () => told.push('pong'),
    onProbeFrame: () => told.push('frame'),
    onProbeClosed: () => told.push('closed'),
  })
  const took = performance.now() - started
  expect(probed).toBe(null)
  expect(took).toBeLessThan(WS_PROBE_TIMEOUT_MS / 3)
  await new Promise((resolve) => setTimeout(resolve, 100))
  expect(told).toEqual([])
  transport.dispose()
})
