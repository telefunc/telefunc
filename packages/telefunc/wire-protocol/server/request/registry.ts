export { createRequestReviver }

import type { Reviver } from '@brillout/json-serializer/parse'
import { fileReviver } from './file.js'
import { blobReviver } from './blob.js'
import { functionReviver } from './function.js'
import { readableStreamReviver } from './readable-stream.js'
import type { ServerReviverContext } from '../../types.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { assert } from '../../../utils/assert.js'
import { isObject } from '../../../utils/isObject.js'
assertIsNotBrowser()

/** File before Blob — File extends Blob, so must be checked first. */
const serverRequestTypes = [fileReviver, blobReviver, readableStreamReviver, functionReviver]

function createRequestReviver(context: ServerReviverContext): Reviver {
  return (_key: undefined | string, value: string, parser: (str: string) => unknown) => {
    for (const type of serverRequestTypes) {
      if (value.startsWith(type.prefix)) {
        const metadata = parser(value.slice(type.prefix.length))
        assert(isObject(metadata))
        return { replacement: type.createValue(metadata as never, context).value }
      }
    }
    return undefined
  }
}
