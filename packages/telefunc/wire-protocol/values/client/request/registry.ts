export { createRequestReplacer }
export type { ChannelCarriedType, Claim, Replaced }

import type { ClientReplacerContext, ReplacerType, TypeContract } from '../../types.js'
import type { AbortError } from '../../../../shared/Abort.js'
import type { ConnectionOptions } from '../../../channel/client/ClientChannel.js'
import type { Features } from '../loadFeatures.js'
import { fileReplacer } from './file.js'
import { blobReplacer } from './blob.js'
import { functionType } from './function.js'
import { readableStreamType } from './readableStream.js'

type ClientReplacerType = ReplacerType<TypeContract, ClientReplacerContext>

/** A built-in value carried over a channel of its own. The call claims it at its invocation, with the page's eager
 *  code, and opens its channel with channels' code: at once on a page that has it. */
type ChannelCarriedType<C extends TypeContract = TypeContract> = {
  prefix: string
  detect(value: unknown): value is C['value']
  claim(value: C['value']): Claim
}
type Claim = {
  metadata: { channelId: string }
  open(channels: NonNullable<Features['channels']>, connection: ConnectionOptions): Omit<Replaced, 'value'>
  /** The call ends unsent before the channel opened: lets go of what the claim took. */
  release(reason: unknown): void
}
/** A value whose tag is written and whose resources are live: the call ties them to its own end. */
type Replaced = { value: unknown; close: () => Promise<void> | void; abort: (abortError: AbortError) => void }

/** Carried in the request's own body. File before Blob — File extends Blob, so must be checked first. */
const bodyTypes: ClientReplacerType[] = [fileReplacer, blobReplacer]
/** Carried over a channel of their own. */
const channelTypes: ChannelCarriedType[] = [readableStreamType, functionType]

function createRequestReplacer(
  context: ClientReplacerContext,
  onReplaced: (replaced: Replaced) => void,
  onClaimed: (value: unknown, claim: Claim) => void,
  extensionTypes: ClientReplacerType[],
) {
  const replace = (type: ClientReplacerType, value: unknown, serializer: (v: unknown) => string) => {
    const { metadata, close, abort } = type.replace(value, context)
    onReplaced({ value, close, abort })
    return { replacement: type.prefix + serializer(metadata), resolved: true }
  }
  return (_key: string, value: unknown, serializer: (v: unknown) => string) => {
    for (const type of bodyTypes) if (type.detect(value)) return replace(type, value, serializer)
    for (const type of channelTypes) {
      if (!type.detect(value)) continue
      const claim = type.claim(value)
      onClaimed(value, claim)
      return { replacement: type.prefix + serializer(claim.metadata), resolved: true }
    }
    for (const type of extensionTypes) if (type.detect(value)) return replace(type, value, serializer)
    return undefined
  }
}
