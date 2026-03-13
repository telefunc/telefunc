export { createRequestReviver }

import type { Reviver } from '@brillout/json-serializer/parse'
import { fileServerType } from './file.js'
import { blobServerType } from './blob.js'
import { functionServerRequestType } from './function.js'
import type { ServerRequestType, RequestBodyReader } from '../../request-types.js'
import type { PlaceholderServerReviverType, PlaceholderTypeContract } from '../../placeholder-types.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { assert } from '../../../utils/assert.js'
import { isObject } from '../../../utils/isObject.js'
assertIsNotBrowser()

/** File before Blob — same detection order as client-side. */
const serverRequestTypes: ServerRequestType[] = [fileServerType, blobServerType]
const serverRequestPlaceholderTypes: PlaceholderServerReviverType<PlaceholderTypeContract>[] = [
  functionServerRequestType,
]

function createRequestReviver(reader: RequestBodyReader): Reviver {
  return (_key: undefined | string, value: string, parser: (str: string) => unknown) => {
    for (const type of serverRequestTypes) {
      if (value.startsWith(type.prefix)) {
        const metadata = parser(value.slice(type.prefix.length))
        assert(isObject(metadata))
        return { replacement: type.createValue(metadata, reader) }
      }
    }
    for (const type of serverRequestPlaceholderTypes) {
      if (value.startsWith(type.prefix)) {
        const metadata = parser(value.slice(type.prefix.length))
        assert(isObject(metadata))
        return { replacement: type.createValue(metadata).value }
      }
    }
    return undefined
  }
}
