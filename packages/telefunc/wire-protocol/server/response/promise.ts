export { promiseServerType }

import { stringify } from '@brillout/json-serializer/stringify'
import { isPromise } from '../../../utils/isPromise.js'
import { textEncoder } from '../../frame.js'
import { SERIALIZER_PREFIX_PROMISE } from '../../constants.js'
import type { ServerStreamingType, PromiseContract } from '../../streaming-types.js'
import type { ServerResponseContext } from './registry.js'

const promiseServerType: ServerStreamingType<PromiseContract, ServerResponseContext> = {
  prefix: SERIALIZER_PREFIX_PROMISE,
  detect: (value): value is Promise<unknown> => isPromise(value),
  getMetadata: (_value, _context) => ({}),
  createProducer: (value) => {
    return {
      chunks: (async function* () {
        yield textEncoder.encode(stringify(await value))
      })(),
      cancel: () => {
        // Promises can't be cancelled
      },
    }
  },
}
