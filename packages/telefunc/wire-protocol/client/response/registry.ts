export { clientStreamingTypes, createStreamingReviver }

import type { Reviver } from '@brillout/json-serializer/parse'
import { asyncGeneratorClientType } from './async-generator.js'
import { readableStreamClientType } from './readable-stream.js'
import { promiseClientType } from './promise.js'
import { channelClientPlaceholderType } from './channel.js'
import type { ClientStreamingType } from '../../streaming-types.js'
import type { ClientPlaceholderType } from '../../placeholder-types.js'
import { assert } from '../../../utils/assert.js'
import { isObject } from '../../../utils/isObject.js'
import { ClientChannel } from '../channel.js'

const clientStreamingTypes: ClientStreamingType[] = [
  asyncGeneratorClientType,
  readableStreamClientType,
  promiseClientType,
]

const clientPlaceholderTypes: ClientPlaceholderType[] = [channelClientPlaceholderType]

/**
 * Creates a JSON-serializer reviver that reconstructs streaming values and
 * placeholder values (e.g. Channel) from prefixed metadata placeholders.
 *
 * Streaming types consume chunks from the HTTP response body. The server injects
 * an explicit `__index` into each metadata object to deterministically map each
 * value to its chunk reader.
 *
 * Placeholder types are reconstructed from metadata only — no chunk consumption.
 */
function createStreamingReviver(
  getChunkReader: (index: number) => () => Promise<Uint8Array | null>,
  getCancelIndex: (index: number) => () => void,
  shard?: string,
) {
  const channels: ClientChannel[] = []
  const reviver: Reviver = (_key: undefined | string, value: string, parser: (str: string) => unknown) => {
    for (const type of clientStreamingTypes) {
      if (value.startsWith(type.prefix)) {
        const metadata = parser(value.slice(type.prefix.length))
        assert(isObject(metadata))
        const index = metadata.__index
        assert(typeof index === 'number')
        delete metadata.__index
        const liveValue = type.createValue(metadata, getChunkReader(index), getCancelIndex(index))
        return { replacement: liveValue }
      }
    }
    return revivePlaceholder(value, parser, channels, shard)
  }
  return { reviver, channels }
}

function revivePlaceholder(value: string, parser: (str: string) => unknown, channels: ClientChannel[], shard?: string) {
  for (const type of clientPlaceholderTypes) {
    if (value.startsWith(type.prefix)) {
      const metadata = parser(value.slice(type.prefix.length))
      assert(isObject(metadata))
      const liveValue = type.createValue(metadata, shard)
      if (liveValue instanceof ClientChannel) channels.push(liveValue)
      return { replacement: liveValue }
    }
  }
  return undefined
}
