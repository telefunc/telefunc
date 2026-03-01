export { promiseClientType }

import { parse } from '@brillout/json-serializer/parse'
import { textDecoder } from '../frame.js'
import type { ClientStreamingType } from './interface.js'

const promiseClientType: ClientStreamingType = {
  prefix: '!TelefuncPromise:',
  createValue: (
    _metadata: unknown,
    readNextChunk: () => Promise<Uint8Array | null>,
    cancel: () => void,
  ): Promise<unknown> => {
    console.log('[client:promise] createValue — waiting for chunk')
    return readNextChunk().then((chunk) => {
      // Signal completion so the demuxer knows this consumer is done.
      // Critical for per-tag cancel: upstream is only cancelled when ALL
      // consumers are done, so promises must report completion too.
      console.log(`[client:promise] got chunk (${chunk?.byteLength ?? 'null'} bytes), calling cancel()`)
      cancel()
      if (!chunk) throw new Error('Stream ended before promise resolved')
      return parse(textDecoder.decode(chunk))
    })
  },
}
