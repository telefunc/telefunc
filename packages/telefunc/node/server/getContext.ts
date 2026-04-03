export { getContext }
export { provideTelefuncContext }
export { restoreContext }
export { installAsyncMode }
export { isAsyncMode }
export type { Telefunc }

import { getContext_sync, provideTelefuncContext_sync, restoreContext_sync } from './getContext/sync.js'
import { assert, assertWarning } from '../../utils/assert.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { isObject } from '../../utils/isObject.js'
import { getRequestContext } from './requestContext.js'
import type { Telefunc } from './getContext/TelefuncNamespace.js'

type GetContext = () => Telefunc.Context
type ProvideTelefuncContext = (context: Telefunc.Context) => void
type RestoreContext = <T>(context: null | Telefunc.Context, fn: () => T) => T

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
  /** Register a callback that fires when the request lifecycle ends for any reason
   *  (response sent, stream complete, or client disconnect). Fires exactly once. */
  onClose: (cb: () => void) => void
  /** AbortSignal that fires when the request lifecycle ends. Same timing as onClose. */
  signal: AbortSignal
}

function augmentContext(context: Record<string, unknown>): void {
  const reqCtx = getRequestContext()
  if (!reqCtx) {
    // SSR implementation not trivial
    context.onClose = () => {}
    context.signal = new AbortController().signal
    return
  }
  context.onClose = (cb: () => void) => reqCtx.onClose(cb)
  context.signal = reqCtx.signal
}

function provideTelefuncContext<Context extends object = Telefunc.Context>(context: Context): void {
  /* TO-DO/eventually: check whether it's possible to deprecate Async Hooks for Nuxt.
  assertWarning(false, 'provideTelefuncContext() is deprecated', { onlyOnce: true })
  */
  assert(isObject(context))
  globalObject.provideTelefuncContext(context)
}

function restoreContext<T>(context: null | Telefunc.Context, fn: () => T): T {
  assert(context === null || isObject(context))
  return globalObject.restoreContext(context, fn)
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
