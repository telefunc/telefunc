export { serializeTelefunctionArguments }

import { stringify } from '@brillout/json-serializer/stringify'
import { assert, assertUsage } from '../../utils/assert.js'
import { hasProp } from '../../utils/hasProp.js'
import { lowercaseFirstLetter } from '../../utils/lowercaseFirstLetter.js'
import { createFileReplacer } from '../../shared/multipart/serializer-client.js'

type CallContext = {
  telefuncFilePath: string
  telefunctionName: string
  telefunctionArgs: unknown[]
  telefuncUrl: string
}

function serializeTelefunctionArguments(callContext: CallContext): string | Blob {
  const dataMain = {
    file: callContext.telefuncFilePath,
    name: callContext.telefunctionName,
    args: callContext.telefunctionArgs,
  }

  const files: { index: number; value: File | Blob }[] = []
  const replacer = createFileReplacer({
    onFile: (index, file) => files.push({ index, value: file }),
    onBlob: (index, blob) => files.push({ index, value: blob }),
  })

  const dataMainSerialized = serialize(dataMain, callContext, replacer)
  if (files.length === 0) return dataMainSerialized

  // Build binary frame: [u32 metadata length][metadata UTF-8][file0 bytes][file1 bytes]...
  const metadataBytes = new TextEncoder().encode(dataMainSerialized)
  const lengthPrefix = new Uint8Array(4)
  new DataView(lengthPrefix.buffer).setUint32(0, metadataBytes.length, false)
  return new Blob([lengthPrefix, metadataBytes, ...files.map((f) => f.value)])
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
