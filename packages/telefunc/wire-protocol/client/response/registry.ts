export { createStreamingReviver }

import type { Reviver } from '@brillout/json-serializer/parse'
import { asyncGeneratorReviver } from './async-generator.js'
import { readableStreamReviver } from './readable-stream.js'
import { promiseReviver } from './promise.js'
import { pubsubReviver } from './pubsub.js'
import { channelReviver } from './channel.js'
import { functionReviver } from './function.js'
import type { ClientReviverContext, ReviverType, TypeContract } from '../../types.js'
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
  extensionTypes: ReviverType<TypeContract, ClientReviverContext>[],
) {
  const allTypes = [...clientTypes, ...extensionTypes]
  const reviver: Reviver = (_path, value, parser) => {
    for (const type of allTypes) {
      if (value.startsWith(type.prefix)) {
        const metadata = parser(value.slice(type.prefix.length))
        assert(isObject(metadata))
        const revived = type.revive(metadata as never, context)
        onRevived(revived)
        return { replacement: revived.value }
      }
    }
    return undefined
  }
  return reviver
}
