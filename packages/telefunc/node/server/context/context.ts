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

/**
 * Runs `fn` with `context` as the current context, the one `getContext()` reads.
 *
 * Why: `getContext()` takes no argument, so Telefunc has to say whose context is current. Without AsyncLocalStorage
 * there is one current context at a time (one variable on `globalThis`), not one per request. `restoreContext()` sets it
 * for the synchronous part of `fn` only and puts the previous value back when `fn` returns, so that code running later,
 * for another request, cannot read it.
 *
 * When: Telefunc wraps the telefunction, an extension's `onTelefunctionResult()` hook, a `getContext().onClose()`
 * callback, the `fn` of `provideTelefuncContext(context, fn)`, and the loop that reads a generator or stream the
 * telefunction returned, each pull of that loop and the call that cancels it. Without AsyncLocalStorage it also wraps
 * `useData()` during a server render.
 *
 * With `telefunc/async_hooks` it is `AsyncLocalStorage.run()`, and the context also follows `fn` across its `await`s.
 */
function restoreContext<T>(context: Context, fn: () => T): T {
  globalObject.neverRestored = false
  // TODO/after-merge: with two versions of Telefunc loaded, `globalObject` has the functions of the one loaded first.
  return globalObject.restoreContext(context, fn)
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
