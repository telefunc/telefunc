export { getRawContext }
export { restoreContext }
export { provideContext }
export { installAsyncMode }
export { isAsyncMode }
export type { Context }

import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { assert } from '../../../utils/assert.js'
import { restoreContext_sync, getContext_sync } from './sync.js'
import type { Telefunc } from './TelefuncNamespace.js'

/** Unified context object. Symbol-keyed — each concern owns its own key. */
type Context = {
  [key: symbol]: unknown
}

// ── Storage ────────────────────────────────────────────────────────

type ContextGetter = () => Context | null
type RestoreContext = <T>(context: Context, fn: () => T) => T
type ProvideTelefuncContext = (context: Telefunc.Context) => void

const globalObject = getGlobalObject<{
  getContext: ContextGetter
  restoreContext: RestoreContext
  provideTelefuncContext: ProvideTelefuncContext | null
  neverRestored: boolean
  isAsyncMode: boolean
}>('context.ts', {
  getContext: getContext_sync,
  restoreContext: restoreContext_sync,
  provideTelefuncContext: null,
  neverRestored: true,
  isAsyncMode: false,
})

// ── Public accessors ───────────────────────────────────────────────

/** Get the unified context object. Extensions use this to read/write their own symbol keys. */
function getRawContext(): Context | null {
  return globalObject.getContext()
}

function restoreContext<T>(context: Context, fn: () => T): T {
  globalObject.neverRestored = false
  // With telefunc@0.3.1 loaded first and no Async Hooks, `globalObject.restoreContext` is that version's function: it keeps the context until a timer fires.
  return (isAsyncMode() ? globalObject.restoreContext : restoreContext_sync)(context, fn)
}

function provideContext(context: Telefunc.Context): void {
  assert(globalObject.provideTelefuncContext)
  globalObject.provideTelefuncContext(context)
}

// ── Async mode ─────────────────────────────────────────────────────

function installAsyncMode({
  getContext_async,
  provideTelefuncContext_async,
  restoreContext_async,
}: {
  getContext_async: ContextGetter
  provideTelefuncContext_async: ProvideTelefuncContext
  restoreContext_async: RestoreContext
}): void {
  globalObject.getContext = getContext_async
  globalObject.provideTelefuncContext = provideTelefuncContext_async
  globalObject.restoreContext = restoreContext_async
  globalObject.isAsyncMode = true
}

function isAsyncMode(): boolean {
  return globalObject.isAsyncMode
}
