import { describe, expect, it } from 'vitest'

describe("the server's `telefunc/client`", () => {
  it('takes the config writes that code shared with the client makes, and `telefuncConfig` is `config`', async () => {
    const { config, telefuncConfig } = await import('telefunc/client')
    config.stream.transport = 'sse-inline'
    config.channel.transports = ['sse']
    config.extensions.push({ name: 'server-render' })
    expect(telefuncConfig).toBe(config)
  })
})
