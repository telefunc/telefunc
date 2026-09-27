import { afterEach, describe, expect, it } from 'vitest'

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
