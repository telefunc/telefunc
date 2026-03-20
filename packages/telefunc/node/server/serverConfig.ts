export { configUser as config }
export { getServerConfig }
export { enableChannelTransports }
export type { ConfigUser, ConfigResolved, StreamConfigUser, ChannelConfigUser, ChannelConfigResolved }

import { assertUsage } from '../../utils/assert.js'
import { hasProp } from '../../utils/hasProp.js'
import { isObject } from '../../utils/isObject.js'
import { isTelefuncFilePath } from '../../utils/isTelefuncFilePath.js'
import { toPosixPath, pathIsAbsolute } from '../../utils/path.js'
import {
  CHANNEL_BUFFER_LIMIT_BYTES,
  CHANNEL_CLIENT_REPLAY_BUFFER_BYTES,
  CHANNEL_CONNECT_TTL_MS,
  CHANNEL_IDLE_TIMEOUT_MS,
  CHANNEL_PING_INTERVAL_MS,
  CHANNEL_RECONNECT_TIMEOUT_MS,
  CHANNEL_SERVER_REPLAY_BUFFER_BYTES,
  DEFAULT_SERVER_CHANNEL_TRANSPORTS,
  DEFAULT_STREAM_TRANSPORT,
  SSE_FLUSH_THROTTLE_MS,
  SSE_POST_IDLE_FLUSH_DELAY_MS,
  type ChannelTransports,
  type StreamTransport,
} from '../../wire-protocol/constants.js'

type StreamConfigUser = {
  /**
   * Default transport for streamed telefunction results.
   *
   * - `'binary-inline'` (default): raw binary HTTP response body
   * - `'sse-inline'`: inline SSE response body
   * - `'channel'`: stream over the configured channel transport
   */
  transport?: StreamTransport
}

type ChannelConfigUser = {
  /**
   * Transports enabled on this server for Telefunc channels.
   * Determines which transport(s) clients may connect with or upgrade to.
   *
   * - `['sse']` (default): SSE only
   * - `['sse', 'ws']`: SSE and WebSocket — clients may upgrade to WS
   * - `['ws']`: WebSocket only
   *
   * Connections arriving on a transport not listed here are immediately rejected.
   */
  transports?: ChannelTransports
  /**
   * How long, in milliseconds, the server keeps channel state after a client
   * disconnects while waiting for a reconnect.
   */
  reconnectTimeout?: number
  /**
   * How long, in milliseconds, to keep the shared channel connection alive
   * after the last channel closes.
   */
  idleTimeout?: number
  /**
   * How often, in milliseconds, the client sends a ping for channel health
   * checks. The server considers the client dead after 2x this interval.
   */
  pingInterval?: number
  /**
   * Per-channel replay buffer size, in bytes, kept on the server for
   * reconnect recovery.
   */
  serverReplayBuffer?: number
  /**
   * Per-channel replay buffer size, in bytes, advertised to the client so it
   * can replay recent client-to-server frames after reconnect.
   */
  clientReplayBuffer?: number
  /**
   * How long, in milliseconds, a newly created channel waits for the client to
   * connect before it is closed automatically.
   */
  connectTtl?: number
  /**
   * Maximum number of bytes buffered per channel for server-to-client messages
   * while no client peer is currently attached.
   */
  bufferLimit?: number
  /**
   * When using SSE channels, upstream client-to-server frames are batched for at
   * most this many milliseconds before Telefunc sends a POST.
   *
   * This value also defines the idle window used to detect the first POST after
   * an idle period.
   */
  sseFlushThrottle?: number
  /**
   * When using SSE channels, the first upstream batch POST after an idle period
   * waits at most this many milliseconds before Telefunc flushes it.
   *
   * Idle means no upstream SSE batch POST has started within the current
   * `sseFlushThrottle` window.
   */
  ssePostIdleFlushDelay?: number
}

type ChannelConfigResolved = {
  transports: ChannelTransports
  reconnectTimeout: number
  idleTimeout: number
  pingInterval: number
  serverReplayBuffer: number
  clientReplayBuffer: number
  connectTtl: number
  bufferLimit: number
  sseFlushThrottle: number
  ssePostIdleFlushDelay: number
}

/** Telefunc Server Configuration */
type ConfigUser = {
  /**
   * The Telefunc HTTP endpoint URL, e.g. `/api/_telefunc`.
   *
   * @default /_telefunc
   *
   * https://telefunc.com/telefuncUrl
   */
  telefuncUrl?: string
  /** See https://telefunc.com/event-based#naming-convention */
  disableNamingConvention?: boolean
  /** Your `.telefunc.js` files */
  telefuncFiles?: string[]
  /** Your project root directory, e.g. `/home/alice/code/my-app/` */
  root?: string
  /** Whether to disable ETag cache headers */
  disableEtag?: boolean
  /**
   * Whether to generate shield.
   *
   * https://telefunc.com/shield-config
   * https://telefunc.com/shield
   *
   * @default { dev: false, prod: true }
   */
  shield?:
    | {
        dev?: boolean
        prod?: boolean
      }
    | boolean
  log?: {
    /** Whether to log shield errors */
    shieldErrors?:
      | boolean
      | {
          /** Whether to log shield errors in production */
          prod?: boolean
          /** Whether to log shield errors in development */
          dev?: boolean
        }
  }
  /** Default transport for streamed telefunction results when the client doesn't specify one. */
  stream: StreamConfigUser
  /** Enabled transports and runtime settings for Telefunc channels. */
  channel: ChannelConfigUser
}

type ConfigResolved = {
  telefuncUrl: string
  root: string | null
  disableEtag: boolean
  telefuncFiles: string[] | null
  disableNamingConvention: boolean
  shield: { dev: boolean; prod: boolean }
  log: {
    shieldErrors: { dev: boolean; prod: boolean }
  }
  stream: {
    transport: StreamTransport
  }
  channel: ChannelConfigResolved
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
          const channelConfigNext = { ...configState.channel }
          setChannelConfigValue(channelConfigNext, subProp, val, 'config.channel')
          configState.channel = channelConfigNext
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

function getServerConfig(): ConfigResolved {
  return {
    disableEtag: configState.disableEtag ?? false,
    disableNamingConvention: configState.disableNamingConvention ?? false,
    shield:
      typeof configState.shield === 'boolean'
        ? { dev: configState.shield, prod: configState.shield }
        : { dev: configState.shield?.dev ?? false, prod: configState.shield?.prod ?? true },
    log: {
      shieldErrors: (() => {
        const shieldErrors = configState.log?.shieldErrors ?? {}
        if (typeof shieldErrors === 'boolean') return { dev: true, prod: true }
        return {
          dev: shieldErrors.dev ?? true,
          prod: shieldErrors.prod ?? true,
        }
      })(),
    },
    telefuncUrl: configState.telefuncUrl || '/_telefunc',
    telefuncFiles: (() => {
      if (configState.telefuncFiles) {
        return configState.telefuncFiles.map(toPosixPath)
      }
      return null
    })(),
    root: (() => {
      if (configState.root) {
        return toPosixPath(configState.root)
      }
      if (typeof process == 'undefined' || !hasProp(process, 'cwd')) return null
      return toPosixPath(process.cwd())
    })(),
    stream: {
      transport: configState.stream.transport || DEFAULT_STREAM_TRANSPORT,
    },
    channel: {
      transports: configState.channel.transports ?? [...DEFAULT_SERVER_CHANNEL_TRANSPORTS],
      reconnectTimeout: configState.channel.reconnectTimeout ?? CHANNEL_RECONNECT_TIMEOUT_MS,
      idleTimeout: configState.channel.idleTimeout ?? CHANNEL_IDLE_TIMEOUT_MS,
      pingInterval: configState.channel.pingInterval ?? CHANNEL_PING_INTERVAL_MS,
      serverReplayBuffer: configState.channel.serverReplayBuffer ?? CHANNEL_SERVER_REPLAY_BUFFER_BYTES,
      clientReplayBuffer: configState.channel.clientReplayBuffer ?? CHANNEL_CLIENT_REPLAY_BUFFER_BYTES,
      connectTtl: configState.channel.connectTtl ?? CHANNEL_CONNECT_TTL_MS,
      bufferLimit: configState.channel.bufferLimit ?? CHANNEL_BUFFER_LIMIT_BYTES,
      sseFlushThrottle: configState.channel.sseFlushThrottle ?? SSE_FLUSH_THROTTLE_MS,
      ssePostIdleFlushDelay: configState.channel.ssePostIdleFlushDelay ?? SSE_POST_IDLE_FLUSH_DELAY_MS,
    },
  }
}

/** @internal Push additional transports into the default only if the user hasn't set one. */
function enableChannelTransports(transports: ChannelTransports): void {
  if (!configState.channel.transports) {
    configState.channel.transports = [
      ...new Set([...DEFAULT_SERVER_CHANNEL_TRANSPORTS, ...transports]),
    ] as ChannelTransports
  }
}

function validateUserConfig(prop: string | symbol, val: unknown) {
  if (typeof prop !== 'string') return

  if (prop === 'root') {
    assertUsage(typeof val === 'string', 'config.root should be a string')
    assertUsage(pathIsAbsolute(val), 'config.root should be an absolute path')
    configState.root = val
  } else if (prop === 'telefuncUrl') {
    assertUsage(typeof val === 'string', 'config.telefuncUrl should be a string')
    assertUsage(
      val.startsWith('/'),
      `config.telefuncUrl (server-side) is '${val}' but it should start with '/' (it should be a URL pathname such as '/_telefunc'), see https://telefunc.com/telefuncUrl`,
    )
    configState.telefuncUrl = val
  } else if (prop === 'telefuncFiles') {
    const wrongType = 'config.telefuncFiles should be a list of paths'
    assertUsage(Array.isArray(val), wrongType)
    val.forEach((item: unknown) => {
      assertUsage(typeof item === 'string', wrongType)
      assertUsage(pathIsAbsolute(item), `[config.telefuncFiles] ${item} should be an absolute path`)
      assertUsage(
        isTelefuncFilePath(toPosixPath(item)),
        `[config.telefuncFiles] ${item} doesn't contain \`.telefunc.\``,
      )
    })
    configState.telefuncFiles = val
  } else if (prop === 'disableEtag') {
    assertUsage(typeof val === 'boolean', 'config.disableEtag should be a boolean')
    configState.disableEtag = val
  } else if (prop === 'disableNamingConvention') {
    assertUsage(typeof val === 'boolean', 'config.disableNamingConvention should be a boolean')
    configState.disableNamingConvention = val
  } else if (prop === 'shield') {
    assertUsage(
      typeof val === 'boolean' || (typeof val === 'object' && val !== null),
      'config.shield should be a boolean or object',
    )
    if (typeof val === 'object' && val !== null && 'dev' in val) {
      assertUsage(typeof (val as { dev: unknown }).dev === 'boolean', 'config.shield.dev should be a boolean')
    }
    if (typeof val === 'object' && val !== null && 'prod' in val) {
      assertUsage(typeof (val as { prod: unknown }).prod === 'boolean', 'config.shield.prod should be a boolean')
    }
    configState.shield = val as ConfigUser['shield']
  } else if (prop === 'log') {
    assertUsage(typeof val === 'object' && val !== null, 'config.log should be an object')
    if ('shieldErrors' in val) {
      const shieldErrors = (val as { shieldErrors: unknown }).shieldErrors
      if (typeof shieldErrors === 'boolean') {
        // Boolean is valid
      } else if (isObject(shieldErrors)) {
        if ('dev' in shieldErrors) {
          assertUsage(
            typeof (shieldErrors as { dev: unknown }).dev === 'boolean',
            'config.log.shieldErrors.dev should be a boolean',
          )
        }
        if ('prod' in shieldErrors) {
          assertUsage(
            typeof (shieldErrors as { prod: unknown }).prod === 'boolean',
            'config.log.shieldErrors.prod should be a boolean',
          )
        }
      } else {
        assertUsage(
          false,
          'config.log.shieldErrors should be either a boolean or an object with dev and prod boolean properties',
        )
      }
    }
    configState.log = val as ConfigUser['log']
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
    setChannelConfigValue(channelConfigNext, key, value, 'config.channel')
  }
  return channelConfigNext
}

function setChannelConfigValue(channelConfigNext: ChannelConfigUser, key: string, value: unknown, basePath: string) {
  const configPath = `${basePath}.${key}`
  switch (key) {
    case 'transports':
      channelConfigNext.transports = validateChannelTransports(value, configPath)
      return
    case 'reconnectTimeout':
    case 'idleTimeout':
    case 'pingInterval':
    case 'serverReplayBuffer':
    case 'clientReplayBuffer':
    case 'connectTtl':
    case 'bufferLimit':
    case 'sseFlushThrottle':
    case 'ssePostIdleFlushDelay':
      assertUsage(typeof value === 'number', `\`${configPath}\` should be a number`)
      assertUsage(value >= 0, `\`${configPath}\` should be a non-negative number`)
      ;(channelConfigNext as Record<string, unknown>)[key] = value
      return
    default:
      assertUsage(false, `Unknown ${configPath}`)
  }
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
    `\`${configPath}\` should be an array of transports, e.g. ['sse'] or ['ws']`,
  )
  return val as ChannelTransports
}
