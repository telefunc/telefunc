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

test.each([
  'reconnectTimeout',
  'idleTimeout',
  'pingInterval',
  'connectTtl',
  'sseFlushThrottle',
  'ssePostIdleFlushDelay',
])('channel config refuses a %s longer than a timer waits, which would fire at once', (key) => {
  try {
    expect(() => (config.channel = { [key]: 2 ** 31 })).toThrow('at most 2147483647')
    config.channel = { [key]: 2 ** 31 - 1 }
    expect((getServerConfig().channel as Record<string, unknown>)[key]).toBe(2 ** 31 - 1)
  } finally {
    config.channel = {}
  }
})
