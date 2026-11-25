export { loadFeatures, loadedFeatures }
export type { Feature, Features }

import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { throwAbortError } from '../../../client/remoteTelefunctionCall/errors.js'
import { ConnectionError } from '../../../client/ConnectionError.js'

/** The client code a call needs only for the values it carries, each loaded by the first call that carries one. */
type Features = {
  /** Streams, files and promises a response's own body carries. */
  streams?: typeof import('./streams.js')
}
type Feature = keyof Features

const globalObject = getGlobalObject<{ loaded: Features; loading: { [F in Feature]?: Promise<void> } }>(
  'wire-protocol/values/client/loadFeatures.ts',
  { loaded: {}, loading: {} },
)

function loadedFeatures(): Features {
  return globalObject.loaded
}

/** Loads `features` for a call (see `callWaitsFor()`), each once per page, and again after a load that failed. */
async function loadFeatures(features: Feature[], call: WaitingCall): Promise<Features> {
  await callWaitsFor(call, Promise.all(features.map(loadFeature)))
  return globalObject.loaded
}

const ABORTED = Symbol('ABORTED')

type WaitingCall = { abortController: AbortController; telefunctionName: string; telefuncFilePath: string }

/** A call waiting for the code it needs: an abort rejects at once, as during the call's `fetch()`, and a load that fails
 *  rejects as a call that can't reach its server does. */
async function callWaitsFor<T>(call: WaitingCall, load: Promise<T>): Promise<Awaited<T>> {
  const { signal } = call.abortController
  const aborted = () => throwAbortError(call.telefunctionName, call.telefuncFilePath, undefined)
  if (signal.aborted) aborted()
  let onAbort = () => {}
  const abort = new Promise<typeof ABORTED>((resolve) => {
    onAbort = () => resolve(ABORTED)
    signal.addEventListener('abort', onAbort, { once: true })
  })
  let loaded: Awaited<T> | typeof ABORTED
  try {
    loaded = await Promise.race([load, abort])
  } catch {
    if (signal.aborted) aborted()
    throw new ConnectionError()
  } finally {
    signal.removeEventListener('abort', onAbort)
  }
  if (loaded === ABORTED) return aborted()
  return loaded
}

function loadFeature(feature: Feature): Promise<void> {
  return (globalObject.loading[feature] ??= importFeature(feature))
}

async function importFeature(feature: Feature): Promise<void> {
  const { loaded } = globalObject
  try {
    if (feature === 'streams') loaded.streams = await import('./streams.js')
  } catch (err) {
    // The next call that needs it tries again: the chunk may be back (a deploy, a network that recovered).
    delete globalObject.loading[feature]
    throw err
  }
}
