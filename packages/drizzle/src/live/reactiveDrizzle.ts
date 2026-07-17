export { reactiveDrizzle }
export type { Reactive, LiveOf, LiveNamespace, DbLiveCarrier }

import type { Live } from 'telefunc'
import { acquireCarrier, captureMutation } from './dbLiveRuntime.js'
import { wrapLiveSelect, type ReadCarrier } from './readCapture.js'

// ─────────────────────────────────────────────────────────────────────────────
// Ticket 6 §1 — client `db.live` surface (type transform + PER-REQUEST accessor).
//
// `reactiveDrizzle(baseDb)()` acquires the reactive db at the top of a telefunction (before any await).
// `acquireCarrier()` establishes the per-request minted-token carrier (context-bearing, before the body's
// first await); the acquired db CLOSES OVER it, so every `.live.select()` — even POST-await — uses the
// captured carrier, never ambient `getRawContext` (which nulls in sync mode, the no-async_hooks reality).
// No module-global `.live` that would silently leak a post-await token.
//
// OWNERSHIP: this module owns the CLIENT SURFACE + the type transform ([GEN]). The engine-coupled runtime
// is imported directly and concretely — the carrier lifecycle from `./dbLiveRuntime` (`acquireCarrier` +
// `captureMutation`) and the read-capture engine from `./readCapture` (`wrapLiveSelect`). No install seam
// and no server auto-load: `reactiveDrizzle(db)` wires them at call time.
// ─────────────────────────────────────────────────────────────────────────────

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
 *  through the acquired db's plain `insert/update/delete` (auto-captured), so their types stay Drizzle's. */
type LiveNamespace<TDb> = TDb extends { select: (...args: infer A) => infer R }
  ? { select(...args: A): LiveOf<R> }
  : Record<never, never>

/** The acquired per-request reactive db: plain `TDb` (exact types + behavior) plus `.live`. */
type Reactive<TDb> = TDb & { live: LiveNamespace<TDb> }

/** Opaque per-request carrier: the minted-read-token Set + the write-capture tx-scope, created by the
 *  extension's start hook and captured by the accessor. Its concrete shape belongs to the runtime units;
 *  this surface only threads it. */
type DbLiveCarrier = { readonly __dbLiveCarrier: true }

/** Set up reactive queries for a Drizzle `db` and return a PER-REQUEST accessor. Call the accessor at the
 *  TOP of a telefunction (before any await) to acquire the reactive db for that request; use `.live.*` for
 *  live reads and the plain `insert/update/delete` for auto-captured writes. */
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
