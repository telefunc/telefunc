export type { PlaceholderTypeContract, ServerPlaceholderType, ClientPlaceholderType, ChannelContract }

import type { ServerChannel } from './server/channel.js'
import type { ClientChannel } from './client/channel.js'

/** Shared contract tying server and client plugins for one placeholder type. */
type PlaceholderTypeContract<V = unknown, R = unknown, M extends Record<string, unknown> = Record<string, unknown>> = {
  value: V
  result: R
  metadata: M
}

/** Server-side placeholder type: detected during serialization, replaced with prefix+metadata in JSON. */
type ServerPlaceholderType<C extends PlaceholderTypeContract = PlaceholderTypeContract> = {
  prefix: string
  detect(value: unknown): value is C['value']
  getMetadata(value: C['value']): C['metadata']
}

/** Client-side placeholder type: reconstructs a live value from prefix+metadata during deserialization. */
type ClientPlaceholderType<C extends PlaceholderTypeContract = PlaceholderTypeContract> = {
  prefix: string
  createValue(metadata: C['metadata'], shard?: string): C['result']
}

type ChannelContract = {
  value: ServerChannel
  result: ClientChannel
  metadata: { channelId: string }
}
