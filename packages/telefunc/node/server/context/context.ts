export { getRawContext }
export { restoreContext }
export { provideContext }
export { installAsyncMode }
export { isAsyncMode }
export type { Context }

import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { restoreContext_sync, provideTelefuncContext_sync, getSyncContext } from './sync.js'
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
  provideTelefuncContext: ProvideTelefuncContext
  neverRestored: boolean
  isAsyncMode: boolean
}>('context.ts', {
  getContext: getSyncContext,
  restoreContext: restoreContext_sync,
  provideTelefuncContext: provideTelefuncContext_sync,
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
  return globalObject.restoreContext(context, fn)
}

function provideContext(context: Telefunc.Context): void {
  globalObject.provideTelefuncContext(context)
}

// ── Async mode ─────────────────────────────────────────────────────

function installAsyncMode({
  provideTelefuncContext_async,
  restoreContext_async,
  getContextStore,
}: {
  provideTelefuncContext_async: ProvideTelefuncContext
  restoreContext_async: RestoreContext
  getContextStore: ContextGetter
}): void {
  globalObject.getContext = getContextStore
  globalObject.restoreContext = restoreContext_async
  globalObject.provideTelefuncContext = provideTelefuncContext_async
  globalObject.isAsyncMode = true
}

function isAsyncMode(): boolean {
  return globalObject.isAsyncMode
}
