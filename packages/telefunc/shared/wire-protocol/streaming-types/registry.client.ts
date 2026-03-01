export { clientStreamingTypes, createStreamingReviver }
export type { StreamingEntry }

import type { Reviver } from '@brillout/json-serializer/parse'
import { asyncGeneratorClientType } from './async-generator.client.js'
import { readableStreamClientType } from './readable-stream.client.js'
import { promiseClientType } from './promise.client.js'
import type { ClientStreamingType } from './interface.js'

const clientStreamingTypes: ClientStreamingType[] = [
  asyncGeneratorClientType,
  readableStreamClientType,
  promiseClientType,
]

type StreamingEntry = {
  type: ClientStreamingType
  metadata: unknown
  index: number
}

/**
 * JSON-serializer reviver that reconstructs streaming values from prefixed
 * metadata placeholders. Each detected entry is recorded in the provided
 * array so the framework can wire up per-tag chunk channels.
 *
 * Absorbs the logic previously in reviver-response.ts, now driven by the
 * registered type plugins.
 */
function createStreamingReviver(
  entries: StreamingEntry[],
  getChunkReader: (tag: number) => () => Promise<Uint8Array | null>,
  getCancelForTag: (tag: number) => () => void,
): Reviver {
  return (_key: undefined | string, value: string, parser: (str: string) => unknown) => {
    for (const type of clientStreamingTypes) {
      if (value.startsWith(type.prefix)) {
        const metadata = parser(value.slice(type.prefix.length))
        const index = (metadata as { index?: number }).index ?? entries.length
        entries.push({ type, metadata, index })
        const liveValue = type.createValue(metadata, getChunkReader(index), getCancelForTag(index))
        return { replacement: liveValue }
      }
    }
    return undefined
  }
}
