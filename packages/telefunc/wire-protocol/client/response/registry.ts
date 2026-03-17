export { clientStreamingTypes, createStreamingReviver }

import type { Reviver } from '@brillout/json-serializer/parse'
import { asyncGeneratorClientType } from './async-generator.js'
import { readableStreamClientType } from './readable-stream.js'
import { promiseClientType } from './promise.js'
import { channelClientPlaceholderType } from './channel.js'
import { functionClientPlaceholderType } from './function.js'
import type { ClientStreamingType } from '../../streaming-types.js'
import type { PlaceholderReviverType } from '../../placeholder-types.js'
import { assert } from '../../../utils/assert.js'
import { isObject } from '../../../utils/isObject.js'
import { isObjectOrFunction } from '../../../utils/isObjectOrFunction.js'
import { ClientChannel } from '../channel.js'

const clientStreamingTypes: ClientStreamingType[] = [
  asyncGeneratorClientType,
  readableStreamClientType,
  promiseClientType,
]

const clientPlaceholderTypes: PlaceholderReviverType[] = [
  //
  channelClientPlaceholderType,
  functionClientPlaceholderType,
]

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
  getChunkReader: (index: number) => () => Promise<Uint8Array<ArrayBuffer> | null>,
  getCancelIndex: (index: number) => () => void,
  shard?: string,
) {
  const channels: ClientChannel[] = []
  const closeHandlers = new WeakMap<object, () => void>()
  const registerClose = (revived: { value: unknown; close: (() => void) | undefined }) => {
    if (!revived.close) return
    assert(isObjectOrFunction(revived.value))
    closeHandlers.set(revived.value, revived.close)
  }
  const reviver: Reviver = (_key: undefined | string, value: string, parser: (str: string) => unknown) => {
    for (const type of clientStreamingTypes) {
      if (value.startsWith(type.prefix)) {
        const metadata = parser(value.slice(type.prefix.length))
        assert(isObject(metadata))
        const index = metadata.__index
        assert(typeof index === 'number')
        delete metadata.__index
        const revived = type.createValue(metadata, getChunkReader(index), getCancelIndex(index))
        registerClose(revived)
        return { replacement: revived.value }
      }
    }
    return revivePlaceholder(value, parser, channels, registerClose, shard)
  }
  return { reviver, channels, closeHandlers }
}

function revivePlaceholder(
  value: string,
  parser: (str: string) => unknown,
  channels: ClientChannel[],
  registerClose: (revived: { value: unknown; close: (() => void) | undefined }) => void,
  shard?: string,
) {
  const context = {
    shard,
    registerChannel(channel: ClientChannel) {
      channels.push(channel)
    },
  }
  for (const type of clientPlaceholderTypes) {
    if (value.startsWith(type.prefix)) {
      const metadata = parser(value.slice(type.prefix.length))
      assert(isObject(metadata))
      const revived = type.createValue(metadata, context)
      registerClose(revived)
      return { replacement: revived.value }
    }
  }
  return undefined
}
