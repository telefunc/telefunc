export { getRawContext }
export { restoreContext }
export { restoreContextInSyncMode }
export { provideContext }
export { installAsyncMode }
export { isAsyncMode }
export type { Context }

import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { restoreContext_sync, provideTelefuncContext_sync, getContext_sync } from './sync.js'
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
  getContext: getContext_sync,
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

/**
 * Runs `fn` with `context` as the context `getContext()` returns.
 *
 * Why: `getContext()` takes no argument, so Telefunc has to say whose context is current. Without AsyncLocalStorage
 * that is one variable for the whole process. It holds a request's context only while Telefunc synchronously calls
 * that request's code, and it gets its previous value back when the call returns, so that code which runs later,
 * for another request, cannot read it.
 *
 * When: around every call Telefunc makes into code that may call `getContext()`, which is the telefunction, an
 * extension's `onTelefunctionResult()` hook, and the loop that reads a generator or stream the telefunction returned.
 *
 * With `telefunc/async_hooks` it is `AsyncLocalStorage.run()`, and the context also follows `fn` across its `await`s.
 * Without it, a call made after an `await` needs its own restore, which is each pull of that loop
 * (`restoreContextInSyncMode()`) and `useData()` during a server render.
 */
function restoreContext<T>(context: Context, fn: () => T): T {
  globalObject.neverRestored = false
  return globalObject.restoreContext(context, fn)
}

/** Without AsyncLocalStorage the context ends with the synchronous call, so a call made after an `await` restores it. */
function restoreContextInSyncMode<T>(context: Context, fn: () => T): T {
  return globalObject.isAsyncMode ? fn() : restoreContext(context, fn)
}

function provideContext(context: Telefunc.Context): void {
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
