export { asyncGeneratorReviver }

import type { ReviverType, AsyncGeneratorContract, ClientReviverContext } from '../../types.js'
import { parse } from '@brillout/json-serializer/parse'
import { textDecoder } from '../../../wire/frame.js'
import { SERIALIZER_PREFIX_GENERATOR } from '../../../constants.js'

const asyncGeneratorReviver: ReviverType<AsyncGeneratorContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_GENERATOR,
  revive: (metadata, context) => {
    const { readNextChunk, cancel, abort } = context.receiveStream(metadata)
    let onEnd = () => {}
    const end = () => {
      cancel()
      onEnd()
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
      // Not methods: on Bun (JavaScriptCore) a method keeps this object alive, and the proxy is put on it (parse.ts).
      close: async () => {
        await gen.return()
      },
      abort,
      onEnd: (callback) => {
        onEnd = callback
      },
    }
  },
}
