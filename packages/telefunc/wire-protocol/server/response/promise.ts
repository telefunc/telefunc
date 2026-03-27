export { promiseReplacer }

import type { StreamingReplacerType, PromiseContract, ServerReplacerContext } from '../../types.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { isPromise } from '../../../utils/isPromise.js'
import { textEncoder } from '../../frame.js'
import { SERIALIZER_PREFIX_PROMISE } from '../../constants.js'

const promiseReplacer: StreamingReplacerType<PromiseContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_PROMISE,
  detect: (value): value is Promise<unknown> => isPromise(value),
  getMetadata: (value, context) => {
    if (context.useChannelPump) return { channelId: context.pumpToChannel(() => promiseReplacer.createProducer(value)) }
    return { __index: context.registerStreamingValue(() => promiseReplacer.createProducer(value)) }
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
