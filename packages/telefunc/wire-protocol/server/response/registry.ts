export { serverStreamingTypes, createStreamingReplacer }

import { asyncGeneratorServerType } from './async-generator.js'
import { readableStreamServerType } from './readable-stream.js'
import { promiseServerType } from './promise.js'
import type { ServerStreamingType, StreamingValueServer } from '../../streaming-types.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const serverStreamingTypes: ServerStreamingType[] = [
  asyncGeneratorServerType,
  readableStreamServerType,
  promiseServerType,
]

/**
 * Creates a JSON-serializer replacer that detects streaming values and replaces
 * them with prefixed metadata placeholders. Returns the replacer function and
 * the collected streaming values.
 */
function createStreamingReplacer() {
  const streamingValues: StreamingValueServer[] = []
  let nextIndex = 0
  const replacer = (_key: string, value: unknown, serializer: (v: unknown) => string) => {
    for (const type of serverStreamingTypes) {
      if (type.detect(value)) {
        const index = nextIndex++
        streamingValues.push({ type, value, index })
        const pluginMeta = type.getMetadata(value)
        return {
          replacement: type.prefix + serializer(pluginMeta),
          resolved: true,
        }
      }
    }
    return undefined
  }
  return { replacer, streamingValues }
}
