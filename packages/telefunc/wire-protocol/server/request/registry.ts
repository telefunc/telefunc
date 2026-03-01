export { createRequestReviver }

import type { Reviver } from '@brillout/json-serializer/parse'
import { fileServerType } from './file.js'
import { blobServerType } from './blob.js'
import type { ServerRequestType, RequestBodyReader } from '../../request-types.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

/** File before Blob — same detection order as client-side. */
const serverRequestTypes: ServerRequestType[] = [fileServerType, blobServerType]

/**
 * JSON-serializer reviver that reconstructs File/Blob values from prefixed
 * metadata placeholders. Uses the provided RequestBodyReader to access
 * the binary body data.
 */
function createRequestReviver(reader: RequestBodyReader): Reviver {
  return (_key: undefined | string, value: string, parser: (str: string) => unknown) => {
    for (const type of serverRequestTypes) {
      if (value.startsWith(type.prefix)) {
        const metadata = parser(value.slice(type.prefix.length)) as Record<string, unknown>
        return { replacement: type.createValue(metadata, reader) }
      }
    }
    return undefined
  }
}
