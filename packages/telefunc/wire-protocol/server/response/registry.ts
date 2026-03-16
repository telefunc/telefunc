export { serverStreamingTypes, createStreamingReplacer }
export type { ServerResponseContext }

import { asyncGeneratorServerType } from './async-generator.js'
import { readableStreamServerType } from './readable-stream.js'
import { promiseServerType } from './promise.js'
import { channelServerPlaceholderType } from './channel.js'
import { functionServerPlaceholderType } from './function.js'
import type { PlaceholderReplacerType, PlaceholderTypeContract } from '../../placeholder-types.js'
import type { ServerStreamingType, StreamingTypeContract, StreamingValueServer } from '../../streaming-types.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { DEFAULT_CHANNEL_TRANSPORT, type ChannelTransport } from '../../constants.js'
assertIsNotBrowser()

type ResponseAbortableChannel = {
  _setResponseAbort(abortResponse: (abortValue?: unknown) => void): void
  abort(abortValue?: unknown): void
}

type ServerResponseContext = {
  channelTransport: ChannelTransport
  registerChannel(channel: ResponseAbortableChannel): void
}

const serverStreamingTypes: ServerStreamingType<StreamingTypeContract, ServerResponseContext>[] = [
  asyncGeneratorServerType,
  readableStreamServerType,
  promiseServerType,
]

const serverPlaceholderTypes: PlaceholderReplacerType<PlaceholderTypeContract, ServerResponseContext>[] = [
  channelServerPlaceholderType,
  functionServerPlaceholderType,
]

/**
 * Creates a JSON-serializer replacer that detects streaming values and placeholder
 * values (e.g. Channel), replacing them with prefixed metadata strings.
 *
 * Streaming types produce chunks over the HTTP response body.
 * Placeholder types are serialization-only — they do NOT produce HTTP streaming chunks.
 *
 * An explicit `__index` is injected into each streaming metadata object so the
 * client can deterministically reconstruct chunk readers without relying on
 * JSON traversal order.
 */
function createStreamingReplacer(
  channelTransport: ChannelTransport = DEFAULT_CHANNEL_TRANSPORT,
  registerChannel: (channel: ResponseAbortableChannel) => void = () => {},
) {
  const streamingValues: StreamingValueServer[] = []
  const context: ServerResponseContext = {
    channelTransport,
    registerChannel,
  }
  let nextIndex = 0
  const replacer = (_key: string, value: unknown, serializer: (v: unknown) => string) => {
    for (const type of serverStreamingTypes) {
      if (type.detect(value)) {
        const index = nextIndex++
        streamingValues.push({ createProducer: () => type.createProducer(value), index })
        const pluginMeta = type.getMetadata(value, context)
        return {
          replacement: type.prefix + serializer({ ...pluginMeta, __index: index }),
          resolved: true,
        }
      }
    }
    for (const type of serverPlaceholderTypes) {
      if (type.detect(value)) {
        return {
          replacement: type.prefix + serializer(type.getMetadata(value, context)),
          resolved: true,
        }
      }
    }
    return undefined
  }
  return { replacer, streamingValues }
}
