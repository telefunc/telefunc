export { configUser as config }
export { resolveClientConfig }
export { getChannelTransport, getStreamTransport }

import { assertUsage, assertWarning } from '../utils/assert.js'
import { isObject } from '../utils/isObject.js'
import {
  DEFAULT_CHANNEL_TRANSPORT,
  DEFAULT_STREAM_TRANSPORT,
  type ChannelTransport,
  type StreamTransport,
} from '../wire-protocol/constants.js'

type StreamConfigUser = {
  transport?: StreamTransport
}

type ChannelConfigUser = {
  transport?: ChannelTransport
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
  stream?: StreamConfigUser
  /** Configuration for Telefunc channels. */
  channel?: ChannelConfigUser
}

type ConfigResolved = {
  telefuncUrl: string
  headers: Record<string, string> | null
  fetch: typeof globalThis.fetch | null
  stream?: StreamConfigUser
  channel?: ChannelConfigUser
}

const configState: ConfigUser = {}

const streamConfig = new Proxy<StreamConfigUser>({} as StreamConfigUser, {
  get(_target, prop) {
    if (prop === 'transport') return configState.stream?.transport
    return undefined
  },
  set(_target, prop, val) {
    if (prop !== 'transport') {
      assertUsage(false, `Unknown config.stream.${String(prop)}`)
    }
    configState.stream ??= {}
    configState.stream.transport = validateStreamTransport(val, 'config.stream.transport')
    return true
  },
})

const channelConfig = new Proxy<ChannelConfigUser>({} as ChannelConfigUser, {
  get(_target, prop) {
    if (prop === 'transport') return configState.channel?.transport
    return undefined
  },
  set(_target, prop, val) {
    if (prop !== 'transport') {
      assertUsage(false, `Unknown config.channel.${String(prop)}`)
    }
    configState.channel ??= {}
    configState.channel.transport = validateChannelTransport(val, 'config.channel.transport')
    return true
  },
})

const configUser: ConfigUser = new Proxy({} as ConfigUser, {
  get(_target, prop) {
    if (prop === 'stream') return streamConfig
    if (prop === 'channel') return channelConfig
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
    stream: configState.stream ? { ...configState.stream } : undefined,
    channel: configState.channel ? { ...configState.channel } : undefined,
  }
}

function getStreamTransport(config: { stream?: StreamConfigUser }): StreamTransport {
  return config.stream?.transport ?? DEFAULT_STREAM_TRANSPORT
}

function getChannelTransport(config: { channel?: ChannelConfigUser }): ChannelTransport {
  return config.channel?.transport ?? DEFAULT_CHANNEL_TRANSPORT
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
    if (key === 'transport') {
      channelConfigNext.transport = validateChannelTransport(value, 'config.channel.transport')
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

function validateChannelTransport(val: unknown, configPath: string): ChannelTransport {
  assertUsage(val === 'sse' || val === 'ws', `\`${configPath}\` should be 'sse' or 'ws'`)
  return val
}

export type { ConfigUser, ConfigResolved, StreamConfigUser, ChannelConfigUser }
