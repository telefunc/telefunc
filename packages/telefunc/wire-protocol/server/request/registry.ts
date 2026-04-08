export { createRequestReviver }

import type { Reviver } from '@brillout/json-serializer/parse'
import { fileReviver } from './file.js'
import { blobReviver } from './blob.js'
import { functionReviver } from './function.js'
import { readableStreamReviver } from './readable-stream.js'
import type { ServerReviverContext, ReviverType, TypeContract } from '../../types.js'
import type { AbortError } from '../../../shared/Abort.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { assert } from '../../../utils/assert.js'
import { isObject } from '../../../utils/isObject.js'
assertIsNotBrowser()

/** File before Blob — File extends Blob, so must be checked first. */
const serverRequestTypes = [fileReviver, blobReviver, readableStreamReviver, functionReviver]

function createRequestReviver(
  context: ServerReviverContext,
  onRevived: (revived: {
    value: unknown
    close: () => Promise<void> | void
    abort: (abortError: AbortError) => void
  }) => void,
  extensionTypes: ReviverType<TypeContract, ServerReviverContext>[],
): Reviver {
  const allTypes = [...serverRequestTypes, ...extensionTypes]
  return (_key: undefined | string, value: string, parser: (str: string) => unknown) => {
    for (const type of allTypes) {
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
}
