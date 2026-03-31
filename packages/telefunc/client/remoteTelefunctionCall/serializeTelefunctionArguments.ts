export { serializeTelefunctionArguments }

import { stringify } from '@brillout/json-serializer/stringify'
import { assert, assertUsage } from '../../utils/assert.js'
import { hasProp } from '../../utils/hasProp.js'
import { lowercaseFirstLetter } from '../../utils/lowercaseFirstLetter.js'
import { createRequestReplacer } from '../../wire-protocol/client/request/registry.js'
import { encodeRequestEnvelope } from '../../wire-protocol/frame.js'
import { pumpClientProducerToChannel } from '../../wire-protocol/client/request/pumpToChannel.js'
import { makeAbortError } from './errors.js'
import type { ClientReplacerContext } from '../../wire-protocol/types.js'
import type { ChannelTransports, StreamTransport } from '../../wire-protocol/constants.js'

type CallContext = {
  telefuncFilePath: string
  telefunctionName: string
  telefunctionArgs: unknown[]
  stream?: { transport?: StreamTransport }
  channel: { transports: ChannelTransports }
  abortController: AbortController
  extensions?: Record<string, unknown>
}

function serializeTelefunctionArguments(callContext: CallContext): string | Blob {
  const dataMain: Record<string, unknown> = {
    file: callContext.telefuncFilePath,
    name: callContext.telefunctionName,
    args: callContext.telefunctionArgs,
  }

  if (callContext.stream?.transport) {
    const { transport } = callContext.stream
    dataMain.stream = { transport }
  }

  if (callContext.extensions) {
    dataMain.extensions = callContext.extensions
  }

  const channelTransports = callContext.channel.transports
  const abortSignal = callContext.abortController.signal
  const files: Blob[] = []

  const context: ClientReplacerContext = {
    channelTransports,
    registerFile(body) {
      const index = files.length
      files.push(body)
      return index
    },
    registerChannel(channel) {
      abortSignal.addEventListener(
        'abort',
        () => {
          const abortError = makeAbortError(undefined, callContext)
          channel._abortLocally(abortError.abortValue, abortError.message)
        },
        { once: true },
      )
    },
    pumpToChannel(createProducer) {
      const channel = pumpClientProducerToChannel(createProducer, channelTransports)
      context.registerChannel(channel)
      return { channelId: channel.id, close: () => channel.close() }
    },
  }

  const replacer = createRequestReplacer(context)
  const dataMainSerialized = serialize(dataMain, callContext, replacer)
  if (files.length > 0) return encodeRequestEnvelope(dataMainSerialized, files)
  return dataMainSerialized
}

type Replacer = Parameters<typeof stringify>[1] extends infer O ? (O extends { replacer?: infer R } ? R : never) : never
function serialize(dataMain: Record<string, unknown>, callContext: CallContext, replacer?: Replacer): string {
  let serialized: string
  try {
    serialized = stringify(dataMain, { forbidReactElements: true, replacer })
  } catch (err) {
    assert(hasProp(err, 'message', 'string'))
    assertUsage(
      false,
      [
        `Cannot serialize arguments for telefunction ${callContext.telefunctionName}() (${callContext.telefuncFilePath}).`,
        'Make sure that the arguments passed to telefunction calls are always serializable.',
        `Serialization error: ${lowercaseFirstLetter(err.message)}`,
      ].join(' '),
    )
  }
  assert(serialized)
  return serialized
}
