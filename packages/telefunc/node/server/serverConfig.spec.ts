import { afterEach, describe, expect, it, test } from 'vitest'

import { config, enableChannelTransports, getServerConfig } from './serverConfig.js'

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

test.each(['reconnectTimeout', 'idleTimeout', 'connectTtl', 'sseFlushThrottle', 'ssePostIdleFlushDelay'])(
  'channel config refuses a %s longer than a timer waits, which would fire at once',
  (key) => {
    try {
      expect(() => (config.channel = { [key]: 2 ** 31 })).toThrow('at most 2147483647')
      config.channel = { [key]: 2 ** 31 - 1 }
      expect((getServerConfig().channel as Record<string, unknown>)[key]).toBe(2 ** 31 - 1)
    } finally {
      config.channel = {}
    }
  },
)

test('channel config refuses a pingInterval whose deadline, twice it, is longer than a timer waits', () => {
  try {
    expect(() => (config.channel = { pingInterval: 2 ** 30 })).toThrow('at most 1073741823')
    config.channel = { pingInterval: 2 ** 30 - 1 }
    expect(getServerConfig().channel.pingInterval).toBe(2 ** 30 - 1)
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

describe('config.room', () => {
  afterEach(() => {
    config.room = {}
  })
  it('defaults to 300000 and 20000, and takes 0 to 2^31 - 1 ms or Infinity', () => {
    expect(getServerConfig().room).toEqual({ emptyTimeout: 300_000, departureTimeout: 20_000 })
    config.room = { emptyTimeout: Infinity, departureTimeout: 2 ** 31 - 1 }
    config.room.departureTimeout = 0
    expect(getServerConfig().room).toEqual({ emptyTimeout: Infinity, departureTimeout: 0 })
  })
  it.each([-1, 1.5, 2 ** 31, Number.NaN, '1000'])('refuses %s as a usage error', (value) => {
    const message = 'should be a non-negative safe integer of milliseconds, at most 2147483647'
    expect(() => (config.room.emptyTimeout = value as number)).toThrow(`\`config.room.emptyTimeout\` ${message}`)
    expect(() => (config.room = { departureTimeout: value as number })).toThrow(
      `\`config.room.departureTimeout\` ${message}`,
    )
  })
  it('refuses an option it does not have', () => {
    expect(() => (config.room = { timeout: 1 } as never)).toThrow('Unknown config.room.timeout')
  })
})
