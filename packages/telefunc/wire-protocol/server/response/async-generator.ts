export { asyncGeneratorReplacer }

import type { StreamingReplacerType, AsyncGeneratorContract, ServerReplacerContext } from '../../types.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { isAsyncGenerator } from '../../../utils/isAsyncGenerator.js'
import { textEncoder } from '../../frame.js'
import { SERIALIZER_PREFIX_GENERATOR } from '../../constants.js'

const asyncGeneratorReplacer: StreamingReplacerType<AsyncGeneratorContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_GENERATOR,
  detect: (value): value is AsyncGenerator<unknown> => isAsyncGenerator(value),
  getMetadata: (value, context) => {
    if (context.useChannelPump)
      return { channelId: context.pumpToChannel(() => asyncGeneratorReplacer.createProducer(value)) }
    return { __index: context.registerStreamingValue(() => asyncGeneratorReplacer.createProducer(value)) }
  },
  createProducer: (value) => {
    const chunks = (async function* () {
      try {
        while (true) {
          const { done, value: chunk } = await value.next()
          if (done) break
          yield textEncoder.encode(stringify(chunk))
        }
      } finally {
        await value.return(undefined)
      }
    })()
    return {
      chunks,
      cancel: () => {
        chunks.return(undefined)
        value.return(undefined)
      },
    }
  },
}
