export { reactiveDrizzle, _installDbLiveRuntime }
export type { Reactive, LiveOf, LiveNamespace, DbLiveRuntime, DbLiveCarrier }

import type { ClientLive } from 'telefunc'

// ─────────────────────────────────────────────────────────────────────────────
// Ticket 6 §1 — client `db.live` surface (type transform + PER-REQUEST accessor, Option B).
//
// The telefunction acquires the reactive db at the top (before any await) via `reactiveDrizzle(baseDb)()`.
// The extension's `onTelefunctionStart` hook establishes the per-request minted-token carrier (context-
// bearing, before the body's first await); the acquired db CLOSES OVER it, so every `.live.select()` — even
// POST-await — uses the captured carrier, never ambient `getRawContext` (which nulls in sync mode, the
// no-async_hooks reality). No module-global `.live` that would silently leak a post-await token.
//
// OWNERSHIP: this module owns the CLIENT SURFACE + the type transform ([GEN]). The engine-coupled RUNTIME
// (read-capture: builder → acquire → σ-seed → serialize-time activation; write-capture: tx-scoped
// accumulator → router.ingest → Broadcast) is a TYPED DEPENDENCY SEAM (`DbLiveRuntime`) IMPLEMENTED BY the
// read-capture + write-capture units and installed via `_installDbLiveRuntime` at extension setup. That
// separation keeps this file free of engine internals (the import-boundary gate) and independently sound.
// ─────────────────────────────────────────────────────────────────────────────

/** A live query: the SAME Drizzle select-builder chain, but awaiting it yields `ClientLive<T[]>` instead
 *  of `T[]`. Every chain method (`from`/`where`/`innerJoin`/…) returns another `LiveOf<…>`, carrying the
 *  live-ness to the terminal `await`; non-await surface (`toSQL`, `getSQL`) is preserved. */
type LiveOf<B> = B extends PromiseLike<infer R>
  ? {
      [K in keyof B]: B[K] extends (...args: infer A) => infer Ret ? (...args: A) => LiveOf<Ret> : B[K]
    } & {
      then<TResult1 = ClientLive<R>, TResult2 = never>(
        onfulfilled?: ((value: ClientLive<R>) => TResult1 | PromiseLike<TResult1>) | null,
        onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
      ): Promise<TResult1 | TResult2>
    }
  : {
      [K in keyof B]: B[K] extends (...args: infer A) => infer Ret ? (...args: A) => LiveOf<Ret> : B[K]
    }

/** The `.live` namespace: the query-producing methods of `TDb` (`select`; `db.query.*` is a fast-follow),
 *  each remapped so its terminal builder awaits to `ClientLive<T[]>`. Mutations are NOT here — writes go
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

/** The engine-coupled db.live RUNTIME — IMPLEMENTED BY the read-capture + write-capture units (Ticket 6
 *  U3+), installed once via `_installDbLiveRuntime`. Keeps this client surface free of engine internals. */
type DbLiveRuntime = {
  /** Per-request carrier acquisition — MUST run before the body's first await (reads the start-hook Set). */
  acquireCarrier(): DbLiveCarrier
  /** Wrap a live SELECT builder: engine acquire (eager hydrate + inert token → carrier) + await-builder
   *  σ-seed → `ClientLive<T[]>`. Returns a live builder whose chain forwards + terminal await captures. */
  wrapLiveSelect(baseBuilder: unknown, carrier: DbLiveCarrier): unknown
  /** Auto-capture a plain mutation for the tx-scoped accumulator → `router.ingest` → Broadcast. */
  captureMutation(
    op: 'insert' | 'update' | 'delete',
    baseMethod: (...a: unknown[]) => unknown,
    carrier: DbLiveCarrier,
  ): (...a: unknown[]) => unknown
}

let runtime: DbLiveRuntime | null = null
/** @internal — the read-capture + write-capture units install the runtime once (at extension setup). */
function _installDbLiveRuntime(r: DbLiveRuntime): void {
  runtime = r
}
function requireRuntime(): DbLiveRuntime {
  if (runtime === null) {
    throw new Error(
      'reactiveDrizzle: the db.live runtime is not installed — ensure the @telefunc/drizzle server extension is loaded (telefunc.server auto-load).',
    )
  }
  return runtime
}

/** Set up reactive queries for a Drizzle `db` and return a PER-REQUEST accessor. Call the accessor at the
 *  TOP of a telefunction (before any await) to acquire the reactive db for that request; use `.live.*` for
 *  live reads and the plain `insert/update/delete` for auto-captured writes. */
function reactiveDrizzle<TDb extends object>(baseDb: TDb): () => Reactive<TDb> {
  return function acquireReactiveDb(): Reactive<TDb> {
    // Capture the per-request carrier NOW (context-bearing, before the body's first await). The returned
    // proxy closes over it, so `.live.select()` even post-await uses the CAPTURED carrier.
    const carrier = requireRuntime().acquireCarrier()
    return new Proxy(baseDb, {
      get(target, prop, receiver) {
        if (prop === 'live') return liveNamespace(target, carrier)
        if (prop === 'insert' || prop === 'update' || prop === 'delete') {
          const base = Reflect.get(target, prop, receiver) as (...a: unknown[]) => unknown
          return requireRuntime().captureMutation(prop, base.bind(target), carrier)
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
      return requireRuntime().wrapLiveSelect(baseBuilder, carrier)
    },
  } as unknown as LiveNamespace<TDb>
}
