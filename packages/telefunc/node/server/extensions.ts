export type { TelefuncServerExtension, TelefuncExtensionHost, LiveProducer }

import type {
  ReplacerType,
  ReviverType,
  TypeContract,
  ServerReplacerContext,
  ServerReviverContext,
} from '../../wire-protocol/types.js'

type TelefuncServerExtension = {
  name: string
  /** Called ONCE, SYNCHRONOUSLY, when the extension is registered (`config.extensions.push(ext)`) — which
   *  may be at import time OR lazily mid-request (as @telefunc/drizzle registers on first use). Receives the
   *  host toolkit: the sanctioned seam for a server-side integration (e.g. an ORM binding) to reach reactive
   *  primitives — a `Live` producer and per-request cleanup — WITHOUT importing telefunc internals. Stash
   *  the host and use it from your integration's request-time code. */
  setup?: (host: TelefuncExtensionHost) => void
  /** Lifecycle hooks (run in context — `getContext()`/`getRawContext()` work). */
  hooks?: {
    /** After the body resolves (success only), before serialization. Runs ONLY for an extension the
     *  request activated (its `requestExtensions` data is present) — `data` is that per-request payload.
     *  Result hooks chain in registration order — each receives the previous hook's returned result; the
     *  return value replaces the result. */
    onTelefunctionResult?: (ctx: { result: unknown; data: Record<string, unknown> }) => unknown | Promise<unknown>
  }
  /** Custom replacer types for server→client serialization (appended after built-in types). */
  responseTypes?: ReplacerType<TypeContract, ServerReplacerContext>[]
  /** Custom reviver types for client→server deserialization (appended after built-in types). */
  requestTypes?: ReviverType<TypeContract, ServerReviverContext>[]
  /** Custom shield type verifiers. Each entry registers a `shield.type[name]` validator. */
  shieldTypes?: Record<string, (input: unknown) => boolean>
}

/** The toolkit telefunc hands an extension at `setup`. Deliberately small — exactly what a reactive ORM
 *  binding needs and no more (this is the seam a generic integration builds on; drizzle is the first
 *  consumer, not the shape). */
type TelefuncExtensionHost = {
  /**
   * Create a server-side `Live` producer seeded with `initialData`. Return the handle from a telefunction
   * and telefunc serializes it as a `Live<T>` for the client; the integration drives it via the producer
   * verbs below. GUARANTEE: the returned handle is a real telefunc `Live` — the wire replacer recognizes
   * it at serialize time — so nothing else is needed to make it cross the wire.
   */
  createLive<T>(initialData: T): LiveProducer<T>
  /**
   * Register a `cleanup` to run ONCE after the request's execute→serialize pipeline SETTLES, on EVERY
   * outcome — success, a body/shield throw before serialize, or a serialize failure (on failure there may
   * be no serialized response at all; the cleanup still runs). MUST be called inside a telefunction, before
   * the body's first await (while the request context is live) — it binds to THAT request's context, so it
   * fires even after the ambient context has nulled in sync mode. GUARANTEE: exactly-once; a throwing
   * cleanup is isolated and never corrupts the response.
   */
  onRequestCleanup(cleanup: () => void): void
}

/** A server-side `Live` producer, as `host.createLive` returns it. To the telefunction's caller it
 *  serializes as a `Live<T>` (`.data`); the integration additionally drives it: `invalidate()` signals the
 *  client the value is stale (it refetches), and `attachSource` wires a serialize-time activation source
 *  (subscribed when the handle crosses the wire, torn down when the last owning channel closes). */
type LiveProducer<T> = {
  readonly data: T
  invalidate(): void
  attachSource(source: { subscribe(onInvalidate: () => void): () => void }): void
}
