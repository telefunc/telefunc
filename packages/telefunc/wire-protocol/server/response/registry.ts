export { createStreamingReplacer }

import { asyncGeneratorReplacer } from './async-generator.js'
import { readableStreamReplacer } from './readable-stream.js'
import { promiseReplacer } from './promise.js'
import { pubsubReplacer } from './pubsub.js'
import { channelReplacer } from './channel.js'
import { functionReplacer } from './function.js'
import type { ServerReplacerContext } from '../../types.js'
import type { AbortError } from '../../../shared/Abort.js'
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
function createStreamingReplacer(
  context: ServerReplacerContext,
  onReplaced: (replaced: { close: () => Promise<void> | void; abort: (abortError: AbortError) => void }) => void,
) {
  const replacer = (_key: string, value: unknown, serializer: (v: unknown) => string) => {
    for (const type of serverTypes) {
      if (type.detect(value)) {
        const { metadata, close, abort } = type.getMetadata(value as never, context)
        onReplaced({ close, abort })
        return {
          replacement: type.prefix + serializer(metadata),
          resolved: true,
        }
      }
    }
    return undefined
  }
  return replacer
}
