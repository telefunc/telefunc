export { reactiveDrizzle }
export type { LiveOf, DbLiveCarrier }

import type { Live } from 'telefunc'
import { acquireCarrier, captureMutation } from './dbLiveRuntime.js'
import { wrapLiveSelect, type ReadCarrier } from './readCapture.js'

// The `db.live` surface: the type transform, and the per-request accessor behind it.
//
// `reactiveDrizzle(baseDb)()` is called at the top of a telefunction, and the db it returns CLOSES OVER
// that request's carrier. That is what lets `.live.select()` work after an await: it uses the captured
// carrier rather than looking for the ambient request context, which by then may be gone. A module-level
// `.live` would have nothing to capture and would silently attach reads to the wrong request.

/** A live query: the SAME Drizzle select-builder chain, but awaiting it yields `Live<T[]>` instead of
 *  `T[]`. Every chain method (`from`/`where`/`innerJoin`/…) returns another `LiveOf<…>`, carrying the
 *  live-ness to the terminal `await`; non-await surface (`toSQL`, `getSQL`) is preserved. A member that
 *  returns a plain `Promise<rows>` rather than a builder — `.execute()`, `.catch`, `.finally` — is NOT
 *  remapped: `.execute()` runs the query and resolves to PLAIN rows (it is never token-captured), so its
 *  type must stay `Promise<rows>`, not `Live`. db.live tracks ONLY the awaited-builder path. */
type LiveOf<B> = B extends PromiseLike<infer R>
  ? { [K in keyof B]: LiveMember<B[K]> } & {
      then<TResult1 = Live<R>, TResult2 = never>(
        onfulfilled?: ((value: Live<R>) => TResult1 | PromiseLike<TResult1>) | null,
        onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
      ): Promise<TResult1 | TResult2>
    }
  : { [K in keyof B]: LiveMember<B[K]> }

/** Remap ONE builder member for `LiveOf`. A method whose return is a live-carrying drizzle query builder
 *  — identified structurally by the builder's own `.execute()` method (drizzle's `QueryPromise`) — chains
 *  to `LiveOf<…>`; everything else is preserved AS-IS: `.execute()` itself and the Promise methods
 *  (`.catch`/`.finally`) return a plain `Promise`, `toSQL()` a plain object. So the type never falsely
 *  claims liveness for a terminal db.live never captures. */
type LiveMember<M> = M extends (...args: infer A) => infer Ret
  ? Ret extends { execute: (...args: any[]) => any }
    ? (...args: A) => LiveOf<Ret>
    : M
  : M

/** The `.live` namespace: the query-producing methods of `TDb` (`select`; `db.query.*` is a fast-follow),
 *  each remapped so its terminal builder awaits to `Live<T[]>`. Mutations are NOT here — writes go
 *  through the acquired db's plain `insert/update/delete`, so their types stay Drizzle's. */
type LiveNamespace<TDb> = TDb extends { select: (...args: infer A) => infer R }
  ? { select(...args: A): LiveOf<R> }
  : Record<never, never>

/** The acquired per-request reactive db: plain `TDb` (exact types + behavior) plus `.live`. */
type Reactive<TDb> = TDb & { live: LiveNamespace<TDb> }

/** Opaque per-request carrier, acquired by the accessor. Here it is nothing but a brand: its concrete
 *  shape belongs to the runtime units, and this surface only threads it. */
type DbLiveCarrier = { readonly __dbLiveCarrier: true }

/** Set up reactive queries for a Drizzle `db` and return a PER-REQUEST accessor. Call the accessor at the
 *  TOP of a telefunction (before any await) to acquire the reactive db for that request; use `.live.*` for
 *  live reads. Writes go through the plain `insert/update/delete`, unchanged. */
function reactiveDrizzle<TDb extends object>(baseDb: TDb): () => Reactive<TDb> {
  return function acquireReactiveDb(): Reactive<TDb> {
    // Capture the per-request carrier NOW (context-bearing, before the body's first await). The returned
    // proxy closes over it, so `.live.select()` even post-await uses the CAPTURED carrier.
    const carrier = acquireCarrier()
    return new Proxy(baseDb, {
      get(target, prop, receiver) {
        if (prop === 'live') return liveNamespace(target, carrier)
        if (prop === 'insert' || prop === 'update' || prop === 'delete') {
          const base = Reflect.get(target, prop, receiver) as (...a: unknown[]) => unknown
          return captureMutation(prop, base.bind(target), carrier)
        }
        return Reflect.get(target, prop, receiver)
      },
    }) as Reactive<TDb>
  }
}

/** The `.live` namespace over the acquired db, closing over the request carrier. */
function liveNamespace<TDb extends object>(db: TDb, carrier: DbLiveCarrier): LiveNamespace<TDb> {
  return {
    select(...args: unknown[]) {
      const baseBuilder = (db as { select: (...a: unknown[]) => unknown }).select(...args)
      return wrapLiveSelect(baseBuilder, carrier as unknown as ReadCarrier, db)
    },
  } as unknown as LiveNamespace<TDb>
}
