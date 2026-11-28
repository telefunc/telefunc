export { loadFeatures, loadedFeatures, loadedFeature, loadedWithExtensions }
export type { Feature, Features }

import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { assertUsage } from '../../../utils/assert.js'
import { abortable } from '../../../client/remoteTelefunctionCall/errors.js'
import { ConnectionError } from '../../../client/ConnectionError.js'

/** The client code a call needs only for the values it carries, each loaded by the first call that carries one, unless
 *  an extension registered its capability with the page. */
type Features = {
  /** Streams, files and promises a response's own body carries. */
  streams?: typeof import('./streams.js')
  /** Channels with their transports: what a returned channel or function, and a stream that rides a channel, need. */
  channels?: typeof import('./channels.js')
  broadcasts?: typeof import('./broadcasts.js')
}
type Feature = keyof Features

const globalObject = getGlobalObject<{ loaded: Features; loading: { [F in Feature]?: Promise<void> } }>(
  'wire-protocol/values/client/loadFeatures.ts',
  { loaded: {}, loading: {} },
)

function loadedFeatures(): Features {
  return globalObject.loaded
}

/** Loads `features` for a call, each once per page: an abort rejects at once, as during the call's `fetch()`, and a load
 *  that fails rejects with `ConnectionError`, as a call that can't reach its server does; the next call that needs it
 *  calls `import()` again. */
async function loadFeatures(features: Feature[], call: WaitingCall): Promise<Features> {
  await abortable(call, async () => {
    try {
      await Promise.all(features.map((feature) => (globalObject.loading[feature] ??= importFeature(feature))))
    } catch {
      throw new ConnectionError()
    }
  })
  return globalObject.loaded
}

/** A loaded feature's code: an extension's value creates its channel or broadcast with what the extension loaded. */
function loadedFeature<F extends 'channels' | 'broadcasts'>(feature: F): NonNullable<Features[F]> {
  const loaded = globalObject.loaded[feature]
  assertUsage(
    loaded,
    `A Telefunc extension's value creates a ${feature === 'channels' ? 'channel' : 'broadcast'}, yet the page hasn't loaded Telefunc's code for it: the extension's client code should \`import 'telefunc/__internal/${feature}'\`.`,
  )
  return loaded
}

/** The code that an entry of `telefunc/__internal` imports statically, for the extensions that import it. */
function loadedWithExtensions(features: {
  channels?: Features['channels']
  broadcasts?: Features['broadcasts']
}): void {
  Object.assign(globalObject.loaded, features)
}

type WaitingCall = { abortController: AbortController; telefunctionName: string; telefuncFilePath: string }

async function importFeature(feature: Feature): Promise<void> {
  const { loaded } = globalObject
  try {
    if (feature === 'streams') loaded.streams = await import('./streams.js')
    if (feature === 'channels') loaded.channels = await import('./channels.js')
    if (feature === 'broadcasts') loaded.broadcasts = await import('./broadcasts.js')
  } catch (err) {
    // The next call that needs it calls import() again, which loads the chunk in browsers that retry a failed module fetch (whatwg/html#10327).
    delete globalObject.loading[feature]
    throw err
  }
}
