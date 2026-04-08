export { serializeTelefunctionArguments }

import { stringify } from '@brillout/json-serializer/stringify'
import { assert, assertUsage } from '../../utils/assert.js'
import { hasProp } from '../../utils/hasProp.js'
import { lowercaseFirstLetter } from '../../utils/lowercaseFirstLetter.js'
import { createRequestReplacer } from '../../wire-protocol/client/request/registry.js'
import { encodeRequestEnvelope } from '../../wire-protocol/frame.js'
import { pumpClientProducerToChannel } from '../../wire-protocol/client/request/pumpToChannel.js'
import { ClientChannel } from '../../wire-protocol/client/channel.js'
import { isObjectOrFunction } from '../../utils/isObjectOrFunction.js'
import { makeAbortError } from './errors.js'
import type { ChannelTransports, StreamTransport } from '../../wire-protocol/constants.js'
import type { ReplacerType, TypeContract, ClientReplacerContext } from '../../wire-protocol/types.js'
import { CloseHandler } from '../close.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { GcRegistry } from '../../wire-protocol/gcRegistry.js'

const globalObject = getGlobalObject('client/remoteTelefunctionCall/serializeTelefunctionArguments.ts', {
  gcRegistry: new GcRegistry(),
})

type CallContext = {
  telefuncFilePath: string
  telefunctionName: string
  telefunctionArgs: unknown[]
  stream?: { transport?: StreamTransport }
  channel: { transports: ChannelTransports }
  abortController: AbortController
  extensions?: Record<string, unknown>
  extensionRequestTypes: ReplacerType<TypeContract, ClientReplacerContext>[]
}

type SerializeResult = {
  httpRequestBody: string | Blob
  requestCloseHandlers: CloseHandler[]
}

function serializeTelefunctionArguments(callContext: CallContext): SerializeResult {
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
  const requestCloseHandlers: CloseHandler[] = []

  const replacer = createRequestReplacer(
    {
      registerFile(body) {
        const index = files.length
        files.push(body)
        return index
      },
      createChannel(opts) {
        return new ClientChannel({
          channelId: crypto.randomUUID(),
          ack: opts?.ack,
          transports: channelTransports,
          defer: true,
        })
      },
      sendStream(createProducer) {
        return pumpClientProducerToChannel(createProducer, channelTransports)
      },
    },
    function onReplaced(replaced) {
      {
        // Track the user's actual value — when they drop all references to it, close.
        // (Unlike the response side, we don't create a value here; the user already
        //  holds the original, so it serves as its own GC anchor.)
        const { value, close } = replaced
        assert(isObjectOrFunction(value))
        globalObject.gcRegistry.register(value, close)
      }

      {
        const { close, abort } = replaced
        abortSignal.addEventListener(
          'abort',
          () => {
            abort(makeAbortError(undefined, callContext))
          },
          { once: true },
        )
        requestCloseHandlers.push(close)
      }
    },
    callContext.extensionRequestTypes,
  )
  const dataMainSerialized = serialize(dataMain, callContext, replacer)
  const httpRequestBody = files.length > 0 ? encodeRequestEnvelope(dataMainSerialized, files) : dataMainSerialized
  return { httpRequestBody, requestCloseHandlers }
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
