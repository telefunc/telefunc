export { createStreamingReviver }

import type { Reviver } from '@brillout/json-serializer/parse'
import { asyncGeneratorReviver } from './async-generator.js'
import { readableStreamReviver } from './readable-stream.js'
import { promiseReviver } from './promise.js'
import { pubsubReviver } from './pubsub.js'
import { channelReviver } from './channel.js'
import { functionReviver } from './function.js'
import type { ClientReviverContext } from '../../types.js'
import type { AbortError } from '../../../shared/Abort.js'
import { assert } from '../../../utils/assert.js'
import { isObject } from '../../../utils/isObject.js'

const clientTypes = [
  asyncGeneratorReviver,
  readableStreamReviver,
  promiseReviver,
  pubsubReviver,
  channelReviver,
  functionReviver,
]

/** Creates a JSON-serializer reviver that delegates to type-specific plugins. */
function createStreamingReviver(
  context: ClientReviverContext,
  onRevived: (revived: {
    value: unknown
    close: () => Promise<void> | void
    abort: (abortError: AbortError) => void
  }) => void,
) {
  const reviver: Reviver = (_key: undefined | string, value: string, parser: (str: string) => unknown) => {
    for (const type of clientTypes) {
      if (value.startsWith(type.prefix)) {
        const metadata = parser(value.slice(type.prefix.length))
        assert(isObject(metadata))
        const revived = type.createValue(metadata as never, context)
        onRevived(revived)
        return { replacement: revived.value }
      }
    }
    return undefined
  }
  return reviver
}
