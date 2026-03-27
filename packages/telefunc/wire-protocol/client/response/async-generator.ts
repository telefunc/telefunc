export { asyncGeneratorReviver }

import type { ReviverType, AsyncGeneratorContract, ClientReviverContext } from '../../types.js'
import { parse } from '@brillout/json-serializer/parse'
import { textDecoder } from '../../frame.js'
import { SERIALIZER_PREFIX_GENERATOR } from '../../constants.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'

const globalObject = getGlobalObject('wire-protocol/client/response/async-generator.ts', {
  gcRegistry: new FinalizationRegistry<() => void>((cancel) => cancel()),
})

const asyncGeneratorReviver: ReviverType<AsyncGeneratorContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_GENERATOR,
  createValue: (metadata, context) => {
    const { readNextChunk, cancel } =
      'channelId' in metadata
        ? context.createChannelChunkReader(metadata.channelId)
        : context.createInlineChunkReader(metadata.__index)
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
    globalObject.gcRegistry.register(gen, cancel)
    return {
      value: gen,
      close: () => gen.return(),
    }
  },
}
