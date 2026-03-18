export { promiseClientType }

import { parse } from '@brillout/json-serializer/parse'
import { textDecoder } from '../../frame.js'
import { SERIALIZER_PREFIX_PROMISE } from '../../constants.js'
import type { ClientStreamingType, PromiseContract } from '../../streaming-types.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'

const globalObject = getGlobalObject('wire-protocol/client/response/promise.ts', {
  gcRegistry: new FinalizationRegistry<() => void>((cancel) => cancel()),
})

const promiseClientType: ClientStreamingType<PromiseContract> = {
  prefix: SERIALIZER_PREFIX_PROMISE,
  createValue: (_metadata, readNextChunk, cancel) => {
    const promise = readNextChunk().then((chunk) => {
      // Signal completion so the demuxer knows this consumer is done.
      // Critical for per-index cancel: upstream is only cancelled when ALL
      // consumers are done, so promises must report completion too.
      cancel()
      if (!chunk) throw new Error('Stream ended before promise resolved')
      return parse(textDecoder.decode(chunk))
    })
    // Suppress unhandled-rejection noise for multiplexed promises a caller never awaits.
    promise.catch(() => {})
    globalObject.gcRegistry.register(promise, cancel)
    return {
      value: promise,
      // A promise has no post-resolution live resource to tear down.
      // Pre-settlement interruption is handled by abort(), not close(res).
      close: undefined,
    }
  },
}
