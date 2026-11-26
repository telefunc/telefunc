export { asyncGeneratorReviver }

import type { ReviverType, AsyncGeneratorContract, ClientReviverContext } from '../../types.js'
import { parse } from '@brillout/json-serializer/parse'
import { textDecoder } from '../../../wire/envelope.js'
import { SERIALIZER_PREFIX_GENERATOR } from '../../../constants.js'

const asyncGeneratorReviver: ReviverType<AsyncGeneratorContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_GENERATOR,
  revive: (metadata, context) => {
    const { readNextChunk, cancel, abort } = context.receiveStream(metadata)
    let onEnd: (() => void) | null = () => {}
    // An explicit `return()` ends it, then its `finally` would again.
    const end = () => {
      const ended = onEnd
      if (ended === null) return
      onEnd = null
      cancel()
      ended()
    }
    const gen = (async function* () {
      try {
        while (true) {
          const chunk = await readNextChunk()
          if (chunk === null) return
          yield parse(textDecoder.decode(chunk))
        }
      } finally {
        end()
      }
    })()
    const origReturn = gen.return.bind(gen)
    gen.return = (...args: Parameters<(typeof gen)['return']>) => {
      end()
      return origReturn(...args)
    }
    return {
      value: gen,
      async close() {
        await gen.return()
      },
      abort,
      onEnd(callback) {
        if (onEnd !== null) onEnd = callback
      },
    }
  },
}
