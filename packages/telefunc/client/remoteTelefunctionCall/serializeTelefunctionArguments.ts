export { serializeTelefunctionArguments }

import { stringify } from '@brillout/json-serializer/stringify'
import { assert, assertUsage } from '../../utils/assert.js'
import { hasProp } from '../../utils/hasProp.js'
import { lowercaseFirstLetter } from '../../utils/lowercaseFirstLetter.js'
import { createRequestReplacer } from '../../wire-protocol/client/request/registry.js'
import { encodeBinaryRequest } from '../../wire-protocol/client/request/serialize.js'

import { type ChannelTransports, type StreamTransport } from '../../wire-protocol/constants.js'

type CallContext = {
  telefuncFilePath: string
  telefunctionName: string
  telefunctionArgs: unknown[]
  stream?: { transport?: StreamTransport }
  channel: { transports: ChannelTransports }
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

  const channelTransports = callContext.channel.transports
  const { replacer, files } = createRequestReplacer(channelTransports)

  const dataMainSerialized = serialize(dataMain, callContext, replacer)
  if (files.length > 0) return encodeBinaryRequest(dataMainSerialized, files)
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
