export { configUser as config }
export { resolveClientConfig }
export { getStreamTransport }

import { assertUsage, assertWarning } from '../utils/assert.js'
import { isObject } from '../utils/isObject.js'
import {
  DEFAULT_CLIENT_CHANNEL_TRANSPORTS,
  DEFAULT_STREAM_TRANSPORT,
  type ChannelTransports,
  type StreamTransport,
} from '../wire-protocol/constants.js'

type StreamConfigUser = {
  transport?: StreamTransport
}

type ChannelConfigUser = {
  transports?: ChannelTransports
}

/** Telefunc Client Configuration */
type ConfigUser = {
  /**
   * The Telefunc HTTP endpoint URL, for example `https://example.org/_telefunc`.
   *
   * @default /_telefunc
   *
   * https://telefunc.com/telefuncUrl
   */
  telefuncUrl?: string
  /** Additional headers sent along Telefunc HTTP requests */
  headers?: Record<string, string>
  /**
   * @deprecated Use `headers` instead.
   * @see headers
   */
  httpHeaders?: Record<string, string>
  /** Custom fetch implementations */
  fetch?: typeof globalThis.fetch
  /** Configuration for streamed telefunction results. */
  stream: StreamConfigUser
  /** Configuration for Telefunc channels. */
  channel: ChannelConfigUser
}

type ConfigResolved = {
  telefuncUrl: string
  headers: Record<string, string> | null
  fetch: typeof globalThis.fetch | null
  stream?: StreamConfigUser
  channel: { transports: ChannelTransports }
}

type ConfigState = Omit<ConfigUser, 'stream' | 'channel'> & { stream: StreamConfigUser; channel: ChannelConfigUser }

const configState: ConfigState = { stream: {}, channel: {} }

const configUser: ConfigUser = new Proxy({} as ConfigUser, {
  get(_target, prop) {
    if (prop === 'stream') {
      return new Proxy({} as StreamConfigUser, {
        get(_t, subProp) {
          return configState.stream[subProp as keyof StreamConfigUser]
        },
        set(_t, subProp, val) {
          if (typeof subProp !== 'string') return true
          configState.stream = validateStreamConfig({ ...configState.stream, [subProp]: val })
          return true
        },
      })
    }
    if (prop === 'channel') {
      return new Proxy({} as ChannelConfigUser, {
        get(_t, subProp) {
          return configState.channel[subProp as keyof ChannelConfigUser]
        },
        set(_t, subProp, val) {
          if (typeof subProp !== 'string') return true
          configState.channel = validateChannelConfig({ ...configState.channel, [subProp]: val })
          return true
        },
      })
    }
    return configState[prop as keyof ConfigUser]
  },
  set(_target, prop, val) {
    validateUserConfig(prop, val)
    return true
  },
})

function resolveClientConfig(): ConfigResolved {
  return {
    headers: configState.headers ?? configState.httpHeaders ?? null,
    telefuncUrl: configState.telefuncUrl || '/_telefunc',
    fetch: configState.fetch ?? null,
    stream: configState.stream.transport ? { ...configState.stream } : undefined,
    channel: { transports: configState.channel.transports ?? [...DEFAULT_CLIENT_CHANNEL_TRANSPORTS] },
  }
}

function getStreamTransport(config: { stream?: StreamConfigUser }): StreamTransport {
  return (config.stream && config.stream.transport) || DEFAULT_STREAM_TRANSPORT
}

function validateUserConfig(prop: string | symbol, val: unknown) {
  if (typeof prop !== 'string') return

  if (prop === 'telefuncUrl') {
    assertUsage(typeof val === 'string', 'config.telefuncUrl should be a string')
    const isIpAddress = (value: string) => /^\d/.test(value)
    assertUsage(
      val.startsWith('/') || val.startsWith('http') || isIpAddress(val),
      `config.telefuncUrl (client-side) is '${val}' but it should be one of the following: a URL pathname (such as '/_telefunc'), a URL with origin (such as 'https://example.org/_telefunc'), or an IP address (such as '192.158.1.38') — see https://telefunc.com/telefuncUrl`,
    )
    configState.telefuncUrl = val
  } else if (prop === 'headers') {
    assertUsage(
      typeof val === 'object' && val !== null && Object.values(val).every((v) => typeof v === 'string'),
      '`config.headers` should be an object of strings',
    )
    configState.headers = val as Record<string, string>
  } else if (prop === 'httpHeaders') {
    assertWarning(false, '`config.httpHeaders` (client-side) is deprecated, use `config.headers` instead', {
      onlyOnce: true,
      showStackTrace: true,
    })
    assertUsage(
      typeof val === 'object' && val !== null && Object.values(val).every((v) => typeof v === 'string'),
      '`config.httpHeaders` should be an object of strings',
    )
    configState.httpHeaders = val as Record<string, string>
  } else if (prop === 'fetch') {
    assertUsage(typeof val === 'function', '`config.fetch` should be a function')
    configState.fetch = val as typeof globalThis.fetch
  } else if (prop === 'stream') {
    configState.stream = validateStreamConfig(val)
  } else if (prop === 'channel') {
    configState.channel = validateChannelConfig(val)
  } else {
    assertUsage(false, `Unknown config.${prop}`)
  }
}

function validateStreamConfig(val: unknown): StreamConfigUser {
  assertUsage(isObject(val), 'config.stream should be an object')
  const streamConfigNext: StreamConfigUser = {}
  for (const [key, value] of Object.entries(val)) {
    if (key === 'transport') {
      streamConfigNext.transport = validateStreamTransport(value, 'config.stream.transport')
    } else {
      assertUsage(false, `Unknown config.stream.${key}`)
    }
  }
  return streamConfigNext
}

function validateChannelConfig(val: unknown): ChannelConfigUser {
  assertUsage(isObject(val), 'config.channel should be an object')
  const channelConfigNext: ChannelConfigUser = {}
  for (const [key, value] of Object.entries(val)) {
    if (key === 'transports') {
      channelConfigNext.transports = validateChannelTransports(value, 'config.channel.transports')
    } else {
      assertUsage(false, `Unknown config.channel.${key}`)
    }
  }
  return channelConfigNext
}

function validateStreamTransport(val: unknown, configPath: string): StreamTransport {
  assertUsage(
    val === 'binary-inline' || val === 'sse-inline' || val === 'channel',
    `\`${configPath}\` should be 'binary-inline', 'sse-inline', or 'channel'`,
  )
  return val
}

function validateChannelTransports(val: unknown, configPath: string): ChannelTransports {
  assertUsage(
    Array.isArray(val) && val.length > 0 && val.every((v) => v === 'sse' || v === 'ws'),
    `\`${configPath}\` should be an array of transports, e.g. ['sse'] or ['sse', 'ws']`,
  )
  return val as ChannelTransports
}

export type { ConfigUser, ConfigResolved, StreamConfigUser, ChannelConfigUser, ChannelTransports }
