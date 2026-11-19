import { expect, test } from 'vitest'
import { createServer } from 'node:http'
import { Telefunc } from './node.js'
import { getServerConfig } from '../node/server/serverConfig.js'

// An app that serves only `serve({ request })` has no upgrade listener: a page told of WebSocket would try one at every
// RECONCILED, and each attempt would fail.
test('a Node app offers WebSocket once it has an upgrade listener, not before', () => {
  const telefunc = new Telefunc()
  expect(getServerConfig().channel.transports).not.toContain('ws')
  const server = createServer()
  telefunc.installWebSocket(server)
  expect(getServerConfig().channel.transports).toContain('ws')
  expect(server.listenerCount('upgrade')).toBe(1)
})
