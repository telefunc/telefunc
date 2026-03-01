export { asyncGeneratorServerType }

import { stringify } from '@brillout/json-serializer/stringify'
import { isAsyncGenerator } from '../../../utils/isAsyncGenerator.js'
import { textEncoder } from '../../frame.js'
import { SERIALIZER_PREFIX_GENERATOR } from '../../constants.js'
import type { ServerStreamingType, AsyncGeneratorContract } from '../../streaming-types.js'

const asyncGeneratorServerType: ServerStreamingType<AsyncGeneratorContract> = {
  prefix: SERIALIZER_PREFIX_GENERATOR,
  detect: (value): value is AsyncGenerator<unknown> => isAsyncGenerator(value),
  getMetadata: () => ({}),
  createProducer: (value) => {
    const chunks = (async function* () {
      try {
        while (true) {
          console.log('[server:async-gen] calling gen.next()')
          const { done, value: chunk } = await value.next()
          if (done) {
            console.log('[server:async-gen] gen.next() returned done=true')
            break
          }
          console.log('[server:async-gen] gen.next() yielded value, encoding')
          yield textEncoder.encode(stringify(chunk))
        }
      } finally {
        console.log('[server:async-gen] finally block — calling gen.return()')
        await value.return(undefined)
      }
    })()
    return {
      chunks,
      cancel: () => {
        console.log('[server:async-gen] cancel() called')
        chunks.return(undefined)
        value.return(undefined)
      },
    }
  },
}
