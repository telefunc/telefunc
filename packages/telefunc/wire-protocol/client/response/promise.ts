export { promiseReviver }

import type { ReviverType, PromiseContract, ClientReviverContext } from '../../types.js'
import { parse } from '@brillout/json-serializer/parse'
import { textDecoder } from '../../frame.js'
import { SERIALIZER_PREFIX_PROMISE } from '../../constants.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'

const globalObject = getGlobalObject('wire-protocol/client/response/promise.ts', {
  gcRegistry: new FinalizationRegistry<() => void>((cancel) => cancel()),
})

const promiseReviver: ReviverType<PromiseContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_PROMISE,
  createValue: (metadata, context) => {
    const { readNextChunk, cancel } =
      'channelId' in metadata
        ? context.createChannelChunkReader(metadata.channelId)
        : context.createInlineChunkReader(metadata.__index)
    const promise = readNextChunk().then((chunk) => {
      cancel()
      if (!chunk) throw new Error('Stream ended before promise resolved')
      return parse(textDecoder.decode(chunk))
    })
    // Suppress unhandled-rejection noise for multiplexed promises a caller never awaits.
    promise.catch(() => {})
    globalObject.gcRegistry.register(promise, cancel)
    return {
      value: promise,
      close: undefined,
    }
  },
}
