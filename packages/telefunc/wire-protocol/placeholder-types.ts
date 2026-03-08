export type {
  PlaceholderTypeContract,
  PlaceholderReplacerType,
  PlaceholderReviverType,
  ChannelContract,
  FunctionContract,
}

import type { ServerChannel } from './server/channel.js'
import type { ClientChannel } from './client/channel.js'

/** Shared contract tying server and client plugins for one placeholder type. */
type PlaceholderTypeContract<V = unknown, R = unknown, M extends Record<string, unknown> = Record<string, unknown>> = {
  value: V
  result: R
  metadata: M
}

/** Placeholder replacer type: detected during serialization, replaced with prefix+metadata in JSON. */
type PlaceholderReplacerType<C extends PlaceholderTypeContract = PlaceholderTypeContract> = {
  prefix: string
  detect(value: unknown): value is C['value']
  getMetadata(value: C['value']): C['metadata']
}

/** Placeholder reviver type: reconstructs a live value from prefix+metadata during deserialization. */
type PlaceholderReviverType<C extends PlaceholderTypeContract = PlaceholderTypeContract> = {
  prefix: string
  createValue(metadata: C['metadata'], shard?: string): C['result']
}

/** `ack: true` — ack is on by default for this channel. */
type ChannelContract = PlaceholderTypeContract<ServerChannel, ClientChannel, { channelId: string; ack?: true }>

/** A plain function returned from a telefunction — transparently proxied over an ack channel. */
type FunctionContract = PlaceholderTypeContract<
  (...args: unknown[]) => unknown,
  (...args: unknown[]) => Promise<unknown>,
  { channelId: string }
>
