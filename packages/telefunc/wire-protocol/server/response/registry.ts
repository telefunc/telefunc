export { serverStreamingTypes, createStreamingReplacer }

import { asyncGeneratorServerType } from './async-generator.js'
import { readableStreamServerType } from './readable-stream.js'
import { promiseServerType } from './promise.js'
import { channelServerPlaceholderType } from './channel.js'
import { functionServerPlaceholderType } from './function.js'
import type { ServerStreamingType, StreamingValueServer } from '../../streaming-types.js'
import type { PlaceholderReplacerType } from '../../placeholder-types.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { ServerChannel } from '../channel.js'
assertIsNotBrowser()

const serverStreamingTypes: ServerStreamingType[] = [
  asyncGeneratorServerType,
  readableStreamServerType,
  promiseServerType,
]

const serverPlaceholderTypes: PlaceholderReplacerType[] = [channelServerPlaceholderType, functionServerPlaceholderType]

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
function createStreamingReplacer() {
  const streamingValues: StreamingValueServer[] = []
  const returnedChannels: ServerChannel[] = []
  let nextIndex = 0
  const replacer = (_key: string, value: unknown, serializer: (v: unknown) => string) => {
    for (const type of serverStreamingTypes) {
      if (type.detect(value)) {
        const index = nextIndex++
        streamingValues.push({ createProducer: () => type.createProducer(value), index })
        const pluginMeta = type.getMetadata(value)
        return {
          replacement: type.prefix + serializer({ ...pluginMeta, __index: index }),
          resolved: true,
        }
      }
    }
    for (const type of serverPlaceholderTypes) {
      if (type.detect(value)) {
        if (ServerChannel.isServerChannel(value)) {
          returnedChannels.push(value)
        }
        const pluginMeta = type.getMetadata(value)
        return {
          replacement: type.prefix + serializer(pluginMeta),
          resolved: true,
        }
      }
    }
    return undefined
  }
  return { replacer, streamingValues, returnedChannels }
}
