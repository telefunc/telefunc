import { afterEach, describe, expect, it, test } from 'vitest'

import { config, enableChannelTransports, getServerConfig } from './serverConfig.js'
import { ServerChannel, reconnectWindow } from '../../wire-protocol/server/channel.js'

describe('channel transports a server adapter enables', () => {
  afterEach(() => {
    config.channel = {}
  })

  it("survive a config.channel assigned after the adapter, as the Cloudflare page's snippet does", () => {
    enableChannelTransports(['ws'])
    config.channel = { reconnectTimeout: 5_000 }
    expect(getServerConfig().channel.transports).toContain('ws')
  })

  it("yield to the user's own transports", () => {
    enableChannelTransports(['ws'])
    config.channel = { transports: ['sse'] }
    expect(getServerConfig().channel.transports).toEqual(['sse'])
  })
})

test.each([Infinity, 0.5, Number.MAX_SAFE_INTEGER + 1])('channel config rejects %s', (value) => {
  expect(() => (config.channel.reconnectTimeout = value)).toThrow('non-negative safe integer')
})

test("channel config refuses a reconnect window longer than a timer waits, which a Room's close and a stream's close wait", () => {
  const longest = 2 ** 31 - 1 - 1_000
  try {
    expect(() => (config.channel = { reconnectTimeout: 2 ** 31 })).toThrow(`at most ${longest} ms`)
    expect(() => (config.channel = { reconnectTimeout: longest - 9_000, pingInterval: 5_000 })).toThrow(
      `at most ${longest} ms`,
    )
    config.channel = { reconnectTimeout: longest - 10_000, pingInterval: 5_000 }
    expect(reconnectWindow()).toBe(longest)
    const channel = new ServerChannel()
    expect(() => channel.close({ timeout: reconnectWindow() })).not.toThrow()
    channel.abort()
  } finally {
    config.channel = {}
  }
})

describe('config.broadcast', () => {
  it('rejects an undefined config.broadcast.transport as a usage error', () => {
    expect(() => {
      config.broadcast.transport = undefined
    }).toThrow('config.broadcast.transport must be a BroadcastTransport')
  })
  it('rejects a Broadcast transport missing a binary method when it is configured', () => {
    const textOnly = { send: () => ({ seq: 1, timestamp: 1 }), listen: () => () => {}, listenBinary: () => () => {} }
    expect(() => {
      config.broadcast = { transport: textOnly as never }
    }).toThrow('config.broadcast.transport must be a BroadcastTransport with send(), listen(), sendBinary()')
  })
})
