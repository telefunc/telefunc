export { clientStreamingTypes, createStreamingReviver }

import type { Reviver } from '@brillout/json-serializer/parse'
import { asyncGeneratorClientType } from './async-generator.js'
import { readableStreamClientType } from './readable-stream.js'
import { promiseClientType } from './promise.js'
import type { ClientStreamingType } from '../../streaming-types.js'
import { assert } from '../../../utils/assert.js'
import { isObject } from '../../../utils/isObject.js'

const clientStreamingTypes: ClientStreamingType[] = [
  asyncGeneratorClientType,
  readableStreamClientType,
  promiseClientType,
]

/**
 * Creates a JSON-serializer reviver that reconstructs streaming values from
 * prefixed metadata placeholders.
 */
function createStreamingReviver(
  getChunkReader: (index: number) => () => Promise<Uint8Array | null>,
  getCancelIndex: (index: number) => () => void,
) {
  let nextIndex = 0
  const reviver: Reviver = (_key: undefined | string, value: string, parser: (str: string) => unknown) => {
    for (const type of clientStreamingTypes) {
      if (value.startsWith(type.prefix)) {
        const metadata = parser(value.slice(type.prefix.length))
        assert(isObject(metadata))
        const index = nextIndex++
        const liveValue = type.createValue(metadata, getChunkReader(index), getCancelIndex(index))
        return { replacement: liveValue }
      }
    }
    return undefined
  }
  return { reviver }
}
