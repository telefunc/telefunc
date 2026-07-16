export { onPostSerialize, drainPostSerializeDisposers }

import type { Context } from './context.js'

// A GENERIC, DB.live-agnostic telefunc-core seam (Ticket 6, R1): run registered disposers ONCE after the
// response is serialized — on the success path AND the serialize-failure path. Scoped to the request's
// `context` object (NOT an ambient `getRawContext()` lookup, which nulls at the first macrotask in sync
// mode): the registrar captures the context at a context-bearing point (e.g. `onTelefunctionStart`), and
// the drain — in `serializeTelefunctionResult` — holds the SAME object via `runContext.context`. This is
// how the reactive-drizzle binding releases db.live read tokens that were minted but never activated
// (never serialized / result removed / serialize failed), without any db.live-specific code in core.

const DISPOSERS = Symbol.for('telefunc.postSerializeDisposers')

/** Register a disposer to run once after this request's response is serialized. Bound to the SPECIFIC
 *  `context` object so the post-serialize drain (which holds the same object) sees it. */
function onPostSerialize(context: Context, disposer: () => void): void {
  const list = (context[DISPOSERS] as Array<() => void> | undefined) ?? []
  list.push(disposer)
  context[DISPOSERS] = list
}

/** Run + clear the registered disposers EXACTLY once. A throwing disposer is isolated (logged, never
 *  propagated) so one failure can't strand the others or corrupt the response. Idempotent: a second
 *  drain is a no-op. */
function drainPostSerializeDisposers(context: Context): void {
  const list = context[DISPOSERS] as Array<() => void> | undefined
  if (!list) return
  context[DISPOSERS] = undefined
  for (const disposer of list) {
    try {
      disposer()
    } catch (err) {
      console.error('[telefunc] a post-serialize disposer threw (isolated):', err)
    }
  }
}
