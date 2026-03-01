export { promiseServerType }

import { stringify } from '@brillout/json-serializer/stringify'
import { isPromise } from '../../../utils/isPromise.js'
import { textEncoder } from '../../frame.js'
import { SERIALIZER_PREFIX_PROMISE } from '../../constants.js'
import type { ServerStreamingType, PromiseContract } from '../../streaming-types.js'

const promiseServerType: ServerStreamingType<PromiseContract> = {
  prefix: SERIALIZER_PREFIX_PROMISE,
  detect: (value): value is Promise<unknown> => isPromise(value),
  getMetadata: () => ({}),
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
