export { asyncGeneratorReviver }

import type { ReviverType, AsyncGeneratorContract, ClientReviverContext } from '../../types.js'
import { parse } from '@brillout/json-serializer/parse'
import { textDecoder } from '../../frame.js'
import { SERIALIZER_PREFIX_GENERATOR } from '../../constants.js'

const asyncGeneratorReviver: ReviverType<AsyncGeneratorContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_GENERATOR,
  createValue: (metadata, context) => {
    const { readNextChunk, cancel, abort } = context.receiveStreamReader(metadata)
    const gen = (async function* () {
      try {
        while (true) {
          const chunk = await readNextChunk()
          if (chunk === null) return
          yield parse(textDecoder.decode(chunk))
        }
      } finally {
        cancel()
      }
    })()
    const origReturn = gen.return.bind(gen)
    gen.return = (...args: Parameters<(typeof gen)['return']>) => {
      cancel()
      return origReturn(...args)
    }
    return {
      value: gen,
      async close() {
        await gen.return()
      },
      abort,
    }
  },
}
