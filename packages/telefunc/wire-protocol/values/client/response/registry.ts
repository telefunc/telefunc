export { createStreamingReviver, featuresIn }

import type { Reviver } from '@brillout/json-serializer/parse'
import type { ClientReviverContext, ReviverType, TypeContract } from '../../types.js'
import { assert } from '../../../../utils/assert.js'
import { isObject } from '../../../../utils/isObject.js'
import type { Feature, Features } from '../loadFeatures.js'
import { channelReviver } from './channel.js'
import { functionReviver } from './function.js'
import {
  SERIALIZER_PREFIX_STEM,
  SERIALIZER_PREFIX_GENERATOR,
  SERIALIZER_PREFIX_STREAM,
  SERIALIZER_PREFIX_FILE,
  SERIALIZER_PREFIX_BLOB,
  SERIALIZER_PREFIX_FILE_DOWNLOAD,
  SERIALIZER_PREFIX_BLOB_DOWNLOAD,
  SERIALIZER_PREFIX_PROMISE,
  SERIALIZER_PREFIX_BROADCAST,
} from '../../../constants.js'

/** The code each built-in value's reviver is in. */
const FEATURE_OF_PREFIX = new Map<string, Feature>([
  [SERIALIZER_PREFIX_GENERATOR, 'streams'],
  [SERIALIZER_PREFIX_STREAM, 'streams'],
  [SERIALIZER_PREFIX_FILE, 'streams'],
  [SERIALIZER_PREFIX_BLOB, 'streams'],
  [SERIALIZER_PREFIX_FILE_DOWNLOAD, 'streams'],
  [SERIALIZER_PREFIX_BLOB_DOWNLOAD, 'streams'],
  [SERIALIZER_PREFIX_PROMISE, 'streams'],
  [SERIALIZER_PREFIX_BROADCAST, 'broadcasts'],
])
const TAG_START = `"${SERIALIZER_PREFIX_STEM}`

/** The code the built-in values in a response's serialized `text` need. The serializer escapes an app's string that
 *  starts with `!` as `!!`, so `"!Telefunc` starts only a value's tag, at any depth. */
function featuresIn(text: string): Set<Feature> {
  const features = new Set<Feature>()
  for (let at = text.indexOf(TAG_START); at !== -1; at = text.indexOf(TAG_START, at + TAG_START.length)) {
    const feature = FEATURE_OF_PREFIX.get(text.slice(at + 1, text.indexOf(':', at) + 1))
    if (feature) features.add(feature)
  }
  return features
}

/** Creates a JSON-serializer reviver that delegates to type-specific plugins, with the built-in ones of `loaded`.
 *
 *  Duplicated references are deduplicated, mirroring createStreamingReplacer: the server emits
 *  one replacement string per value identity, so equal wire strings denote the same server-side
 *  value — the first occurrence revives (side effects run once), duplicates resolve to the
 *  already-revived object, and server-side `===` holds on the client too. */
function createStreamingReviver(
  context: ClientReviverContext,
  handOut: (revived: ReturnType<ReviverType['revive']>) => unknown,
  extensionTypes: ReviverType<TypeContract, ClientReviverContext>[],
  loaded: Features,
) {
  const allTypes: ReviverType<TypeContract, ClientReviverContext>[] = [
    ...(loaded.streams?.responseTypes ?? []),
    ...(loaded.broadcasts?.responseTypes ?? []),
    channelReviver,
    functionReviver,
    ...extensionTypes,
  ]
  const revivedByWireString = new Map<string, unknown>()
  const reviver: Reviver = (_path, value, parser) => {
    if (revivedByWireString.has(value)) return { replacement: revivedByWireString.get(value) }
    for (const type of allTypes) {
      if (value.startsWith(type.prefix)) {
        const metadata = parser(value.slice(type.prefix.length))
        assert(isObject(metadata))
        const revived = type.revive(metadata, context)
        // Duplicates resolve to the exact object the first occurrence handed out.
        const handedOut = handOut(revived)
        revivedByWireString.set(value, handedOut)
        return { replacement: handedOut }
      }
    }
    return undefined
  }
  return reviver
}
