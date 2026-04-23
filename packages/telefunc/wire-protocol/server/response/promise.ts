export { promiseReplacer }

import type { StreamingReplacerType, PromiseContract, ServerReplacerContext } from '../../types.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { isPromise } from '../../../utils/isPromise.js'
import { textEncoder } from '../../frame.js'
import { SERIALIZER_PREFIX_PROMISE } from '../../constants.js'

const promiseReplacer: StreamingReplacerType<PromiseContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_PROMISE,
  detect: (value): value is Promise<unknown> => isPromise(value),
  replace: (value, context) => {
    const { metadata, close, abort } = context.sendStream(() => promiseReplacer.createProducer(value))
    return { metadata, close, abort }
  },
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
