export { ResponseValues }
export type { FeatureResponseTypes }

import { parse } from '@brillout/json-serializer/parse'
import type { ClientReviverContext, ReviverType, TypeContract } from '../../types.js'
import { assert } from '../../../../utils/assert.js'
import { isObject } from '../../../../utils/isObject.js'
import type { Feature, Features } from '../loadFeatures.js'
import { replaceParsedValues } from './replaceParsedValues.js'
import {
  SERIALIZER_PREFIX_STEM,
  SERIALIZER_PREFIX_GENERATOR,
  SERIALIZER_PREFIX_STREAM,
  SERIALIZER_PREFIX_FILE,
  SERIALIZER_PREFIX_BLOB,
  SERIALIZER_PREFIX_FILE_DOWNLOAD,
  SERIALIZER_PREFIX_BLOB_DOWNLOAD,
  SERIALIZER_PREFIX_PROMISE,
  SERIALIZER_PREFIX_CHANNEL,
  SERIALIZER_PREFIX_FUNCTION,
  SERIALIZER_PREFIX_BROADCAST,
} from '../../../constants.js'

/** The feature of each built-in tag: a feature's module revives each of its tags, and no other
 *  (`FeatureResponseTypes`). */
const FEATURE_OF_PREFIX = {
  [SERIALIZER_PREFIX_GENERATOR]: 'streams',
  [SERIALIZER_PREFIX_STREAM]: 'streams',
  [SERIALIZER_PREFIX_FILE]: 'streams',
  [SERIALIZER_PREFIX_BLOB]: 'streams',
  [SERIALIZER_PREFIX_FILE_DOWNLOAD]: 'streams',
  [SERIALIZER_PREFIX_BLOB_DOWNLOAD]: 'streams',
  [SERIALIZER_PREFIX_PROMISE]: 'streams',
  [SERIALIZER_PREFIX_CHANNEL]: 'channels',
  [SERIALIZER_PREFIX_FUNCTION]: 'channels',
  [SERIALIZER_PREFIX_BROADCAST]: 'broadcasts',
} as const satisfies Record<string, Feature>
type BuiltInPrefix = keyof typeof FEATURE_OF_PREFIX
type PrefixOf<F extends Feature> = {
  [P in BuiltInPrefix]: (typeof FEATURE_OF_PREFIX)[P] extends F ? P : never
}[BuiltInPrefix]

/** The revivers of a feature's module, by the tags `FEATURE_OF_PREFIX` gives the feature. */
type FeatureResponseTypes<F extends Feature> = Record<PrefixOf<F>, ClientResponseType>
type LoadedResponseTypes = { [F in Feature]?: { responseTypes: FeatureResponseTypes<F> } }

type ClientResponseType = ReviverType<TypeContract, ClientReviverContext>
type TagType = { prefix: BuiltInPrefix; feature: Feature } | { prefix: string; extension: ClientResponseType }
type ResponseTag = { type: TagType; metadata: unknown }

/** The serializer visits value strings, including those inside tag metadata, before any live value is created. */
class ResponseValues {
  private readonly value: unknown
  private tags?: Map<string, ResponseTag>
  private features?: Set<Feature>

  constructor(body: string, extensions: ClientResponseType[], streamsRideChannels: boolean) {
    this.value = parse(body, {
      reviver: (_path, wire, parseMetadata) => {
        const existing = this.tags?.get(wire)
        if (existing) return { replacement: existing }

        const type = responseType(wire, extensions)
        if (!type) return undefined
        // An extension's value creates what its own client code loaded (`telefunc/__internal`).
        if ('feature' in type) {
          const features = (this.features ??= new Set())
          features.add(type.feature)
          if (type.feature === 'streams' && streamsRideChannels) features.add('channels')
        }

        const metadata = parseMetadata(wire.slice(type.prefix.length))
        const tag = { type, metadata }
        const tags = (this.tags ??= new Map())
        tags.set(wire, tag)
        return { replacement: tag }
      },
    })
  }

  missingFeatures(loaded: Features): Feature[] | undefined {
    return this.features && [...this.features].filter((feature) => !loaded[feature])
  }

  revive(
    context: ClientReviverContext,
    handOut: (revived: ReturnType<ClientResponseType['revive']>) => unknown,
    loaded: Features,
  ): unknown {
    if (!this.tags) return this.value
    const replacements = new Map<object, unknown>()
    // A tag is stored after its metadata, so nested values revive first and repeated wire tags share one value.
    for (const tag of this.tags.values()) {
      const { type } = tag
      const reviver = 'feature' in type ? builtInReviver(loaded, type.feature, type.prefix) : type.extension
      assert(reviver)
      const metadata = replaceParsedValues(tag.metadata, replacements)
      assert(isObject(metadata))
      replacements.set(tag, handOut(reviver.revive(metadata, context)))
    }
    return replaceParsedValues(this.value, replacements)
  }
}

function responseType(wire: string, extensions: ClientResponseType[]): TagType | undefined {
  if (wire.startsWith(SERIALIZER_PREFIX_STEM)) {
    const prefix = wire.slice(0, wire.indexOf(':') + 1)
    if (isBuiltInPrefix(prefix)) return { prefix, feature: FEATURE_OF_PREFIX[prefix] }
  }
  for (const extension of extensions) {
    if (wire.startsWith(extension.prefix)) return { prefix: extension.prefix, extension }
  }
}

/** `prefix` is empty or starts with the stem, neither a name `Object.prototype` has. */
function isBuiltInPrefix(prefix: string): prefix is BuiltInPrefix {
  return prefix in FEATURE_OF_PREFIX
}

function builtInReviver<F extends Feature>(
  loaded: LoadedResponseTypes,
  feature: F,
  prefix: PrefixOf<F>,
): ClientResponseType | undefined {
  return loaded[feature]?.responseTypes[prefix]
}
