export { getContext }
export { provideTelefuncContext }
export { restoreContext }
export { installAsyncMode }
export { isAsyncMode }
export type { Telefunc }

import { getContext_sync, provideTelefuncContext_sync, restoreContext_sync } from './getContext/sync.js'
import { assert } from '../../utils/assert.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { isObject } from '../../utils/isObject.js'
import { getRequestContext } from './requestContext.js'
import type { Telefunc } from './getContext/TelefuncNamespace.js'

type GetContext = () => Telefunc.Context
type ProvideTelefuncContext = (context: Telefunc.Context) => void
type RestoreContext = (context: null | Telefunc.Context) => void

const globalObject = getGlobalObject<{
  getContext: GetContext
  restoreContext: RestoreContext
  provideTelefuncContext: ProvideTelefuncContext
  isAsyncMode: boolean
}>('getContext.ts', {
  getContext: getContext_sync,
  restoreContext: restoreContext_sync,
  provideTelefuncContext: provideTelefuncContext_sync,
  isAsyncMode: false,
})

function getContext<Context extends object = Telefunc.Context>(): Context & TelefuncBuiltins {
  const context = globalObject.getContext()
  assert(isObject(context))
  augmentContext(context)
  return context as Context & TelefuncBuiltins
}

type TelefuncBuiltins = {
  /** Register a callback that fires when the client aborts the connection (disconnects before the response finishes). */
  onConnectionAbort: (cb: () => void) => void
}

function augmentContext(context: Record<string, unknown>): void {
  const reqCtx = getRequestContext()
  assert(reqCtx)
  const { abortSignal } = reqCtx
  context.onConnectionAbort = (cb: () => void) => {
    // Guard: some server environments (e.g. certain Node.js adapters, edge runtimes) may fire
    // the abort signal even after the response has been fully sent. The `completed` flag ensures
    // callbacks only run for genuine client-initiated disconnects, not false positives from
    // inconsistent signal propagation across environments.
    if (reqCtx.completed) return
    if (abortSignal.aborted) {
      cb()
      return
    }
    abortSignal.addEventListener(
      'abort',
      () => {
        if (!reqCtx.completed) cb()
      },
      { once: true },
    )
  }
}

function provideTelefuncContext<Context extends object = Telefunc.Context>(context: Context): void {
  /* TO-DO/eventually: check whether it's possible to deprecate Async Hooks for Nuxt.
  assertWarning(false, 'provideTelefuncContext() is deprecated', { onlyOnce: true })
  */
  assert(isObject(context))
  globalObject.provideTelefuncContext(context)
}

function restoreContext(context: null | Telefunc.Context): void {
  assert(context === null || isObject(context))
  globalObject.restoreContext(context)
}

function installAsyncMode({
  getContext_async,
  provideTelefuncContext_async,
  restoreContext_async,
}: {
  getContext_async: GetContext
  provideTelefuncContext_async: ProvideTelefuncContext
  restoreContext_async: RestoreContext
}): void {
  globalObject.getContext = getContext_async
  globalObject.restoreContext = restoreContext_async
  globalObject.provideTelefuncContext = provideTelefuncContext_async
  globalObject.isAsyncMode = true
}
function isAsyncMode(): boolean {
  return globalObject.isAsyncMode
}
