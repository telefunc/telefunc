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
        console.log('[server:promise] awaiting promise...')
        const resolved = await value
        console.log('[server:promise] promise resolved, yielding value')
        yield textEncoder.encode(stringify(resolved))
        console.log('[server:promise] producer done')
      })(),
      cancel: () => {
        console.log('[server:promise] cancel() called (no-op)')
        // Promises can't be cancelled
      },
    }
  },
}
