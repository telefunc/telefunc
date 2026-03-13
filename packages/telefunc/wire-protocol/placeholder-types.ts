export type {
  PlaceholderTypeContract,
  PlaceholderReplacerType,
  PlaceholderRevivedValue,
  PlaceholderReviveClose,
  PlaceholderReviverType,
  PlaceholderServerReviverType,
  PlaceholderReviverContext,
  ChannelContract,
  FunctionContract,
}

import type { ServerChannel } from './server/channel.js'
import type { ClientChannel } from './client/channel.js'
import type { ClientReviveClose } from './streaming-types.js'

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

type PlaceholderReviverContext = {
  shard?: string
  registerChannel(channel: ClientChannel): void
}

type PlaceholderReviveClose = ClientReviveClose

type PlaceholderRevivedValue<T> = {
  value: T
  close: PlaceholderReviveClose
}

/** Placeholder reviver type: reconstructs a live value from prefix+metadata during deserialization. */
type PlaceholderReviverType<C extends PlaceholderTypeContract = PlaceholderTypeContract> = {
  prefix: string
  createValue(metadata: C['metadata'], context: PlaceholderReviverContext): PlaceholderRevivedValue<C['result']>
}

type PlaceholderServerReviverType<C extends PlaceholderTypeContract = PlaceholderTypeContract> = {
  prefix: string
  createValue(metadata: C['metadata']): PlaceholderRevivedValue<C['result']>
}

/** `ack: true` — ack is on by default for this channel. */
type ChannelContract = PlaceholderTypeContract<ServerChannel, ClientChannel, { channelId: string; ack?: true }>

/** A plain function returned from a telefunction — transparently proxied over an ack channel. */
type FunctionContract = PlaceholderTypeContract<
  (...args: unknown[]) => unknown,
  (...args: unknown[]) => Promise<unknown>,
  { channelId: string }
>
