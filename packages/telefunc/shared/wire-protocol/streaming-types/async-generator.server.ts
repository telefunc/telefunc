export { asyncGeneratorServerType }

import { stringify } from '@brillout/json-serializer/stringify'
import { isAsyncGenerator } from '../../../utils/isAsyncGenerator.js'
import { textEncoder } from '../frame.js'
import type { ServerStreamingType, StreamingProducer } from './interface.js'

const asyncGeneratorServerType: ServerStreamingType = {
  prefix: '!TelefuncGenerator:',
  detect: (value: unknown): boolean => isAsyncGenerator(value),
  getMetadata: (_value: unknown, index: number) => ({ index }),
  createProducer: (value: unknown): StreamingProducer => {
    const gen = value as AsyncGenerator<unknown>
    const chunks = (async function* () {
      try {
        while (true) {
          console.log('[server:async-gen] calling gen.next()')
          const { done, value: chunk } = await gen.next()
          if (done) {
            console.log('[server:async-gen] gen.next() returned done=true')
            break
          }
          console.log('[server:async-gen] gen.next() yielded value, encoding')
          yield textEncoder.encode(stringify(chunk))
        }
      } finally {
        console.log('[server:async-gen] finally block — calling gen.return()')
        await gen.return(undefined)
      }
    })()
    return {
      chunks,
      cancel: () => {
        console.log('[server:async-gen] cancel() called')
        chunks.return(undefined)
        gen.return(undefined)
      },
    }
  },
}
