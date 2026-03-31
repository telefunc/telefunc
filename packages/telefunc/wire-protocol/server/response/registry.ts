export { createStreamingReplacer }

import { asyncGeneratorReplacer } from './async-generator.js'
import { readableStreamReplacer } from './readable-stream.js'
import { promiseReplacer } from './promise.js'
import { pubsubReplacer } from './pubsub.js'
import { channelReplacer } from './channel.js'
import { functionReplacer } from './function.js'
import type { ServerReplacerContext } from '../../types.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

// pubsubReplacer must come before channelReplacer because ServerPubSub extends ServerChannel
const serverTypes = [
  asyncGeneratorReplacer,
  readableStreamReplacer,
  promiseReplacer,
  pubsubReplacer,
  channelReplacer,
  functionReplacer,
]

/** Creates a JSON-serializer replacer that delegates to type-specific plugins. */
function createStreamingReplacer(context: ServerReplacerContext) {
  const replacer = (_key: string, value: unknown, serializer: (v: unknown) => string) => {
    for (const type of serverTypes) {
      if (type.detect(value)) {
        return {
          replacement: type.prefix + serializer(type.getMetadata(value as never, context)),
          resolved: true,
        }
      }
    }
    return undefined
  }
  return replacer
}
