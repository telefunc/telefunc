export { promiseServerType }

import { stringify } from '@brillout/json-serializer/stringify'
import { isPromise } from '../../../utils/isPromise.js'
import { textEncoder } from '../frame.js'
import type { ServerStreamingType, StreamingProducer } from './interface.js'

const promiseServerType: ServerStreamingType = {
  prefix: '!TelefuncPromise:',
  detect: (value: unknown): boolean => isPromise(value),
  getMetadata: (_value: unknown, index: number) => ({ index }),
  createProducer: (value: unknown): StreamingProducer => {
    return {
      chunks: (async function* () {
        console.log('[server:promise] awaiting promise...')
        const resolved = await (value as Promise<unknown>)
        console.log('[server:promise] promise resolved, yielding value')
        yield textEncoder.encode(stringify(resolved))
        console.log('[server:promise] producer done')
      })(),
      cancel: () => {
        console.log('[server:promise] cancel() called (no-op)')
        // Promises can't be cancelled
      },
    }
  },
}
