export { configUser as config }
export { getServerConfig }
export { getServerExtensionTypes }
export { enableChannelTransports }
export { setAdapterMaxFrameBytes }
export { setAdapterMaxMemory }
export { getAdapterMaxFrameBytes }
export { setRootFromVite }
export { getChannelConfig }
export { pingDeadlineOf }
export { reconnectWindow }
export type {
  ConfigUser,
  ConfigResolved,
  StreamConfigUser,
  ChannelConfigUser,
  ChannelConfigResolved,
  BroadcastConfigUser,
}

import { assertUsage } from '../../utils/assert.js'
import { assertDuration } from '../../utils/assertDuration.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { hasProp } from '../../utils/hasProp.js'
import { isObject } from '../../utils/isObject.js'
import type { TelefuncServerExtension } from './extensions.js'
import { registerShieldType } from './shield.js'
import { isTelefuncFilePath } from '../../utils/isTelefuncFilePath.js'
import { toPosixPath, pathIsAbsolute, assertPosixPath } from '../../utils/path.js'
import {
  installBroadcastAdapter,
  DefaultBroadcastAdapter,
  type BroadcastTransport,
} from '../../wire-protocol/broadcast/broadcastAdapter.js'
import {
  CHANNEL_CONNECT_TTL_MS,
  CHANNEL_HOLD_MAX_BYTES,
  CHANNEL_IDLE_TIMEOUT_MS,
  CHANNEL_PING_INTERVAL_MIN_MS,
  CHANNEL_PING_INTERVAL_MS,
  CHANNEL_RECONNECT_TIMEOUT_MIN_MS,
  CHANNEL_RECONNECT_TIMEOUT_MS,
  CREDIT_WINDOW_INITIAL_BYTES,
  DEFAULT_SERVER_CHANNEL_TRANSPORTS,
  DEFAULT_STREAM_TRANSPORT,
  parseStreamConfig,
  validateChannelTransports,
  SSE_FLUSH_THROTTLE_MS,
  SSE_POST_IDLE_FLUSH_DELAY_MS,
  type ChannelTransports,
  type StreamTransport,
  TIMER_DELAY_MAX_MS,
} from '../../wire-protocol/constants.js'
import { pingDeadline } from '../../wire-protocol/wire/pingDeadline.js'

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

type BroadcastConfigUser = {
  /** Transport for cross-node `Broadcast` delivery. */
  transport?: BroadcastTransport
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
   * The most all channels on this server hold for their clients, in bytes or as `'4g'`, `'512m'`. It's a ceiling: nothing
   * is set aside, and one channel holds at most 320 MiB of it.
   *
   * https://telefunc.com/channel-config
   *
   * @default a quarter of the memory the process may use
   */
  maxMemory?: number | string
  /** The most one channel holds for its client, of its sends and of what awaits acknowledgement, in bytes, within
   *  `maxMemory`. @default 335544320 */
  maxPendingBytes?: number
  /**
   * How long, in milliseconds, a newly created channel waits for the client to
   * connect before it is closed automatically.
   */
  connectTtl?: number
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
  maxMemory: number
  maxPendingBytes: number
  connectTtl: number
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
  /** `Broadcast` configuration. */
  broadcast: BroadcastConfigUser
  /** Registered server extensions. Use `config.extensions.push(ext)` to add. */
  extensions: TelefuncServerExtension[]
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
  extensions: TelefuncServerExtension[]
}

const globalObject = getGlobalObject('serverConfig.ts', {
  config: { stream: {}, channel: {}, broadcast: {}, extensions: [] } as ConfigUser,
  /** Transports a server adapter enables: kept apart from the user's config, which a later assignment replaces. */
  adapterChannelTransports: new Set<ChannelTransports[number]>(),
  /** The largest frame the adapter's runtime takes, where that is less than the server reads. */
  adapterMaxFrameBytes: undefined as number | undefined,
  /** The default of `maxMemory` where the adapter's runtime can't tell what the process may use. */
  adapterMaxMemory: undefined as number | undefined,
  /** The default of `maxMemory` elsewhere: a request reads the config, and this process's memory doesn't change. */
  defaultMaxMemory: undefined as number | undefined,
  /** A channel read `config.channel`, whose values each of its parts then relies on staying as they are. */
  channelConfigInUse: false,
})
const configState = globalObject.config

const configUser: ConfigUser = new Proxy({} as ConfigUser, {
  get(_target, prop) {
    if (prop === 'extensions') {
      return new Proxy(configState.extensions, {
        get(target, subProp, receiver) {
          if (subProp === 'push') {
            return (...exts: TelefuncServerExtension[]) => {
              for (const ext of exts) {
                const idx = target.findIndex((e) => e.name === ext.name)
                if (idx >= 0) {
                  target[idx] = ext
                } else {
                  target.push(ext)
                }
                if (ext.shieldTypes) {
                  for (const [name, verify] of Object.entries(ext.shieldTypes)) {
                    registerShieldType(name, verify)
                  }
                }
              }
              return target.length
            }
          }
          return Reflect.get(target, subProp, receiver)
        },
      })
    }
    if (prop === 'stream') {
      return new Proxy({} as StreamConfigUser, {
        get(_t, subProp) {
          return configState.stream[subProp as keyof StreamConfigUser]
        },
        set(_t, subProp, val) {
          if (typeof subProp !== 'string') return true
          configState.stream = parseStreamConfig({ ...configState.stream, [subProp]: val })
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
          applyChannelConfig({ ...configState.channel, [subProp]: val })
          return true
        },
      })
    }
    if (prop === 'broadcast') {
      return new Proxy({} as ConfigUser['broadcast'], {
        get(_t, subProp) {
          return configState.broadcast[subProp as keyof ConfigUser['broadcast']]
        },
        set(_t, subProp, val) {
          if (typeof subProp !== 'string') return true
          applyBroadcastConfig({ ...configState.broadcast, [subProp]: val })
          return true
        },
      })
    }
    return configState[prop as keyof typeof configState]
  },
  set(_target, prop, val) {
    applyUserConfig(prop, val)
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
      // Doesn't work for Cloudflare Workers — but we can make it work if we want to, see reverted commits at https://github.com/telefunc/telefunc/pull/262
      if (rootFromVite) {
        assertPosixPath(rootFromVite)
        return rootFromVite
      }
      if (typeof process == 'undefined' || !hasProp(process, 'cwd')) return null
      return toPosixPath(process.cwd())
    })(),
    stream: {
      transport: configState.stream.transport || DEFAULT_STREAM_TRANSPORT,
    },
    channel: resolveChannelConfig(configState.channel),
    extensions: configState.extensions,
  }
}

function resolveChannelConfig(channel: ChannelConfigUser): ChannelConfigResolved {
  const reconnectTimeout = channel.reconnectTimeout ?? CHANNEL_RECONNECT_TIMEOUT_MS
  return {
    transports: channel.transports ?? [...DEFAULT_SERVER_CHANNEL_TRANSPORTS, ...globalObject.adapterChannelTransports],
    reconnectTimeout,
    // A long reconnect window doesn't pin an idle wire, and a short one (Cloudflare's) doesn't wait longer than itself.
    idleTimeout: channel.idleTimeout ?? Math.min(reconnectTimeout, CHANNEL_IDLE_TIMEOUT_MS),
    pingInterval: channel.pingInterval ?? CHANNEL_PING_INTERVAL_MS,
    connectTtl: channel.connectTtl ?? CHANNEL_CONNECT_TTL_MS,
    maxMemory:
      (channel.maxMemory as number | undefined) ??
      globalObject.adapterMaxMemory ??
      (globalObject.defaultMaxMemory ??= defaultMaxMemory()),
    maxPendingBytes: channel.maxPendingBytes ?? CHANNEL_HOLD_MAX_BYTES,
    sseFlushThrottle: channel.sseFlushThrottle ?? SSE_FLUSH_THROTTLE_MS,
    ssePostIdleFlushDelay: channel.ssePostIdleFlushDelay ?? SSE_POST_IDLE_FLUSH_DELAY_MS,
  }
}

/** What a process may use where the runtime can't tell. */
const DEFAULT_PROCESS_MEMORY_BYTES = 4 * 1024 * 1024 * 1024

/** A quarter of what the process may use: channels hold bytes outside the JS heap, which the rest of the process needs too. */
function defaultMaxMemory(): number {
  const process = globalThis.process
  const total = process?.getBuiltinModule?.('node:os')?.totalmem() ?? DEFAULT_PROCESS_MEMORY_BYTES
  return Math.floor(Math.min(process?.constrainedMemory?.() || total, total) / 4)
}

/** How long a wire may deliver nothing before it is taken for dead: two ping intervals, each of at least a second. */
function pingDeadlineOf({ pingInterval }: Pick<ChannelConfigResolved, 'pingInterval'>): number {
  return pingDeadline(pingInterval)
}

/** The settings a channel runs by, which can't change from here on. */
function getChannelConfig(): ChannelConfigResolved {
  globalObject.channelConfigInUse = true
  return getServerConfig().channel
}

/** How long a gone client is still held: until its drop is noticed at the ping deadline, then for `reconnectTimeout`. */
function reconnectWindow(): number {
  const c = getChannelConfig()
  return Math.min(TIMER_DELAY_MAX_MS, pingDeadlineOf(c) + c.reconnectTimeout)
}

/** Extension wire types are consumed after user modules may have registered more extensions.
 * Keep them out of the request-start config snapshot and resolve only at the operation that uses them. */
function getServerExtensionTypes() {
  return {
    responseTypes: configState.extensions.flatMap((ext) => ext.responseTypes ?? []),
    requestTypes: configState.extensions.flatMap((ext) => ext.requestTypes ?? []),
  }
}

/** @internal Adds transports to the defaults, which apply while the user sets none. */
function enableChannelTransports(transports: ChannelTransports): void {
  for (const transport of transports) globalObject.adapterChannelTransports.add(transport)
}

/** @internal The runtime ends a WebSocket on a larger message: the page refuses one, told by each channel the server makes and by each RECONCILED. */
function setAdapterMaxFrameBytes(bytes: number | undefined): void {
  globalObject.adapterMaxFrameBytes = bytes
}

/** @internal */
function setAdapterMaxMemory(bytes: number): void {
  globalObject.adapterMaxMemory = bytes
}

function getAdapterMaxFrameBytes(): number | undefined {
  return globalObject.adapterMaxFrameBytes
}

function applyUserConfig(prop: string | symbol, val: unknown) {
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
    configState.stream = parseStreamConfig(val)
  } else if (prop === 'channel') {
    applyChannelConfig(val)
  } else if (prop === 'broadcast') {
    applyBroadcastConfig(val)
  } else if (prop === 'extensions') {
    assertUsage(Array.isArray(val), 'config.extensions should be an array')
    configState.extensions = val as TelefuncServerExtension[]
  } else {
    assertUsage(false, `Unknown config.${prop}`)
  }
}

function applyChannelConfig(val: unknown): void {
  assertUsage(isObject(val), 'config.channel should be an object')
  const next: ChannelConfigUser = {}
  for (const [key, value] of Object.entries(val)) {
    const configPath = `config.channel.${key}`
    switch (key) {
      case 'transports':
        next.transports = validateChannelTransports(value, configPath, "['sse'] or ['ws']")
        break
      case 'pingInterval':
        assertDuration(
          value,
          configPath,
          Math.floor(TIMER_DELAY_MAX_MS / 2),
          'as its deadline, twice it, is at most the longest a timer waits',
        )
        assertUsage(
          value >= CHANNEL_PING_INTERVAL_MIN_MS,
          `\`${configPath}\` should be at least ${CHANNEL_PING_INTERVAL_MIN_MS}: more pings than that flood the wire`,
        )
        next[key] = value
        break
      case 'connectTtl':
        assertDuration(value, configPath, TIMER_DELAY_MAX_MS, 'the longest a timer waits')
        assertUsage(
          value >= CHANNEL_CONNECT_TTL_MS,
          `\`${configPath}\` should be at least ${CHANNEL_CONNECT_TTL_MS}: in less, a channel made while its client's connection was down closes before the client reconnects`,
        )
        next[key] = value
        break
      case 'idleTimeout':
      case 'sseFlushThrottle':
      case 'ssePostIdleFlushDelay':
        assertDuration(value, configPath, TIMER_DELAY_MAX_MS, 'the longest a timer waits')
        next[key] = value
        break
      case 'reconnectTimeout':
        assertDuration(value, configPath, TIMER_DELAY_MAX_MS, 'the longest a timer waits')
        assertUsage(
          value >= CHANNEL_RECONNECT_TIMEOUT_MIN_MS,
          `\`${configPath}\` should be at least ${CHANNEL_RECONNECT_TIMEOUT_MIN_MS}: in less, no client can reconnect`,
        )
        next[key] = value
        break
      case 'maxPendingBytes':
        assertUsage(
          typeof value === 'number' && Number.isSafeInteger(value) && value >= CREDIT_WINDOW_INITIAL_BYTES,
          `\`${configPath}\` should be an integer of at least ${CREDIT_WINDOW_INITIAL_BYTES}: a channel's credit window starts at that`,
        )
        next[key] = value
        break
      case 'maxMemory': {
        const bytes = parseMemorySize(value)
        assertUsage(
          bytes !== null && bytes >= CREDIT_WINDOW_INITIAL_BYTES,
          `\`${configPath}\` should be a number of bytes, or a size such as '4g' or '512m', of at least ${CREDIT_WINDOW_INITIAL_BYTES}: a channel's credit window starts at that`,
        )
        next[key] = bytes
        break
      }
      default:
        assertUsage(false, `Unknown ${configPath}`)
    }
  }
  assertUsage(
    !globalObject.channelConfigInUse ||
      JSON.stringify(resolveChannelConfig(next)) === JSON.stringify(resolveChannelConfig(configState.channel)),
    "`config.channel` can't change once a channel exists, as those already open would keep the old values: set it before the server handles a request",
  )
  configState.channel = next
}

/** `value` in bytes: a whole number, or a size such as `'4g'`; null where it is neither. */
function parseMemorySize(value: unknown): number | null {
  if (typeof value === 'number') return Number.isSafeInteger(value) ? value : null
  const match = typeof value === 'string' ? /^(\d+(?:\.\d+)?)([kmg])$/i.exec(value) : null
  if (!match) return null
  return Math.floor(Number(match[1]) * 1024 ** ('kmg'.indexOf(match[2]!.toLowerCase()) + 1))
}

function applyBroadcastConfig(val: unknown): void {
  assertUsage(isObject(val), 'config.broadcast should be an object')
  for (const [key, value] of Object.entries(val)) {
    if (key === 'transport') {
      assertUsage(
        isObject(value) && typeof (value as any).send === 'function' && typeof (value as any).listen === 'function',
        'config.broadcast.transport must be a BroadcastTransport with send() and listen() methods',
      )
      configState.broadcast.transport = value as BroadcastTransport
      installBroadcastAdapter(() => new DefaultBroadcastAdapter(value as BroadcastTransport))
    } else {
      assertUsage(false, `Unknown config.broadcast.${key}`)
    }
  }
}

let rootFromVite: string | null = null
function setRootFromVite(root: string) {
  rootFromVite = root
}
