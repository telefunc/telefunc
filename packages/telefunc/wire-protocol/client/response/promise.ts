export { promiseReviver }

import type { ReviverType, PromiseContract, ClientReviverContext } from '../../types.js'
import { parse } from '@brillout/json-serializer/parse'
import { textDecoder } from '../../frame.js'
import { SERIALIZER_PREFIX_PROMISE } from '../../constants.js'

const promiseReviver: ReviverType<PromiseContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_PROMISE,
  createValue: (metadata, context) => {
    const { readNextChunk, cancel, abort } = context.receiveStreamReader(metadata)
    const promise = readNextChunk().then((chunk) => {
      cancel()
      if (!chunk) throw new Error('Stream ended before promise resolved')
      return parse(textDecoder.decode(chunk))
    })
    // Suppress unhandled-rejection noise for multiplexed promises a caller never awaits.
    promise.catch(() => {})
    return {
      value: promise,
      close: cancel,
      abort,
    }
  },
}
