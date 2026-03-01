export { serverStreamingTypes, createStreamingReplacer }

import { asyncGeneratorServerType } from './async-generator.server.js'
import { readableStreamServerType } from './readable-stream.server.js'
import { promiseServerType } from './promise.server.js'
import type { ServerStreamingType, StreamingValueServer } from './interface.js'

const serverStreamingTypes: ServerStreamingType[] = [
  asyncGeneratorServerType,
  readableStreamServerType,
  promiseServerType,
]

/**
 * JSON-serializer replacer that detects streaming values and replaces them with
 * prefixed metadata placeholders. Collected streaming values are pushed to the
 * provided array for later frame encoding.
 *
 * Absorbs the logic previously in replacer-response.ts, now driven by the
 * registered type plugins.
 */
function createStreamingReplacer(streamingValues: StreamingValueServer[]) {
  let nextIndex = 0
  return (_key: string, value: unknown, serializer: (v: unknown) => string) => {
    for (const type of serverStreamingTypes) {
      if (type.detect(value)) {
        const index = nextIndex++
        streamingValues.push({ type, value, index })
        const meta = type.getMetadata(value, index)
        return {
          replacement: type.prefix + serializer(meta),
          resolved: true,
        }
      }
    }
    return undefined
  }
}
