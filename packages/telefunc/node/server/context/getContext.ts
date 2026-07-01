export { getContext }
export { provideTelefuncContext }
export { PROVIDED_CONTEXT }
export type { Telefunc }

import { assert, assertUsage } from '../../../utils/assert.js'
import { isObject } from '../../../utils/isObject.js'
import { objectAssign } from '../../../utils/objectAssign.js'
import { getRawContext, provideContext, isAsyncMode } from './context.js'
import { REQUEST_CONTEXT } from './requestContext.js'
import type { RequestContext } from './requestContext.js'
import type { Telefunc } from './TelefuncNamespace.js'

const PROVIDED_CONTEXT: unique symbol = Symbol.for('telefunc.providedContext')

function getContext<Context extends object = Telefunc.Context>(): Context & TelefuncBuiltins {
  const raw = getRawContext()
  if (!raw) {
    if (isAsyncMode()) {
      assertUsage(false, '[getContext()] Make sure to call provideTelefuncContext() before calling getContext()')
    }
    assertUsage(false, '[getContext()] Cannot access context object, see https://telefunc.com/getContext#access')
  }
  const providedContext = (raw[PROVIDED_CONTEXT] ?? {}) as Context
  assert(isObject(providedContext))

  const reqCtx = raw[REQUEST_CONTEXT] as RequestContext | undefined
  const builtins: TelefuncBuiltins = reqCtx
    ? { onClose: (cb) => reqCtx.onClose(cb), signal: reqCtx.signal }
    : // SSR — no request context available
      { onClose: () => {}, signal: new AbortController().signal }

  const context = {}
  objectAssign(context, builtins)
  objectAssign(context, providedContext)
  return context
}

type TelefuncBuiltins = {
  /** Register a callback that fires when the request lifecycle ends for any reason
   *  (response sent, stream complete, or client disconnect). Fires exactly once. */
  onClose: (cb: () => void) => void
  /** AbortSignal that fires when the request lifecycle ends. Same timing as onClose. */
  signal: AbortSignal
}

function provideTelefuncContext<Context extends object = Telefunc.Context>(context: Context): void {
  /* TO-DO/eventually: check whether it's possible to deprecate Async Hooks for Nuxt.
  assertWarning(false, 'provideTelefuncContext() is deprecated', { onlyOnce: true })
  */
  assertUsage(isObject(context), '[provideTelefuncContext()] Argument `context` should be an object')
  provideContext(context as Telefunc.Context)
}
