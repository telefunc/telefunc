export { asyncGeneratorClientType }

import { parse } from '@brillout/json-serializer/parse'
import { textDecoder } from '../frame.js'
import { SERIALIZER_PREFIX_GENERATOR } from '../constants.js'
import type { ClientStreamingType, AsyncGeneratorContract } from './interface.js'

const asyncGeneratorClientType: ClientStreamingType<AsyncGeneratorContract> = {
  prefix: SERIALIZER_PREFIX_GENERATOR,
  createValue: (_metadata, readNextChunk, cancel) => {
    const gen = (async function* () {
      try {
        while (true) {
          console.log('[client:async-gen] calling readNextChunk()')
          const chunk = await readNextChunk()
          if (chunk === null) {
            console.log('[client:async-gen] readNextChunk returned null, done')
            return
          }
          console.log(`[client:async-gen] got chunk (${chunk.byteLength} bytes), yielding`)
          yield parse(textDecoder.decode(chunk))
        }
      } finally {
        console.log('[client:async-gen] finally block — calling cancel()')
        cancel()
      }
    })()
    const origReturn = gen.return.bind(gen)
    gen.return = (...args: Parameters<(typeof gen)['return']>) => {
      console.log('[client:async-gen] gen.return() called by consumer')
      cancel()
      return origReturn(...args)
    }
    return gen
  },
}
