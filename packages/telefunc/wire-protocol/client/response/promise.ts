export { promiseClientType }

import { parse } from '@brillout/json-serializer/parse'
import { textDecoder } from '../../frame.js'
import { SERIALIZER_PREFIX_PROMISE } from '../../constants.js'
import type { ClientStreamingType, PromiseContract } from '../../streaming-types.js'

const promiseClientType: ClientStreamingType<PromiseContract> = {
  prefix: SERIALIZER_PREFIX_PROMISE,
  createValue: (_metadata, readNextChunk, cancel) => {
    return readNextChunk().then((chunk) => {
      // Signal completion so the demuxer knows this consumer is done.
      // Critical for per-index cancel: upstream is only cancelled when ALL
      // consumers are done, so promises must report completion too.
      cancel()
      if (!chunk) throw new Error('Stream ended before promise resolved')
      return parse(textDecoder.decode(chunk))
    })
  },
}
