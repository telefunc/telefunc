// The graph registry: identity + leases/tokens. Graph STATE is keyed by InstanceKey; `acquire`
// dedups on the canonical instance string, so several live queries reading the SAME rows share ONE
// graph. Each new identity compiles its plan FRESH — there is no cross-instance plan cache. Two
// concurrent acquires of a not-yet-created identity share ONE in-flight creation, and a failed
// creation clears the entry so a later acquire retries (no poisoned cache). Refcount = channel leases
// + unredeemed read tokens; a read token holds the initial-read fence and REDEEMS into a lease by
// atomic transfer (no transient zero); refcount 0 destroys immediately and synchronously (no grace,
// no cap, no pool).

export { type Registry, type AcquireRequest, type ReadToken, createRegistry }

import type { GraphPlan, StatefulGraph } from '../compile/compile.js'
import { assertUsage } from '../utils/assert.js'
import type { HydrationExecutor } from './hydrate.js'
import { type LiveGraph, createLiveGraph } from './liveGraph.js'
import { type RoutableGraph, type Router, createRouter } from '../router/changeRouter.js'

type AcquireRequest = {
  instanceKey: string
  tables: string[]
  rlsEnabled: boolean
  /** Compile this query to a plan — run once per new identity (no cross-instance plan cache). */
  compilePlan: () => GraphPlan | Promise<GraphPlan>
  executor: HydrationExecutor
  /** The identity's invalidation sink (wired to the channel later). */
  notify: () => void
}

type Lease = { release(): void }

type ReadToken = {
  readonly instanceKey: string
  /** The graph's invalidation sequence at acquire time (the initial-read fence). */
  readonly seqAtRead: number
  /** Atomically transfer this token's ref into a channel lease (no transient zero). */
  redeem(): Lease
  /** Release an UNREDEEMED token (expire / dispose / read-failure); idempotent. */
  release(): void
}

type AcquireResult = { graph: LiveGraph; token: ReadToken }

type Registry = {
  acquire(request: AcquireRequest): Promise<AcquireResult>
  readonly router: Router
}

type Entry = {
  graph: LiveGraph
  routable: RoutableGraph
  tokens: number
  leases: number
  dispose: () => void
}

function createRegistry(config: { maxStateRowsPerInput: number }): Registry {
  // Per-identity subscriber sets: several acquires of the SAME instanceKey are distinct live
  // subscribers of ONE shared graph (independent channels), so an invalidation must notify EVERY
  // one — a single callback per key would silence all but the last. A subscriber joins when its read
  // token is REDEEMED (serialize-time activation, NOT mint — an un-redeemed token is inert) and
  // leaves on that lease's final release; refcount (leases + tokens) is independent, so the last
  // subscriber out still disposes the graph.
  const sinks = new Map<string, Set<() => void>>()
  const subscribe = (key: string, notify: () => void): (() => void) => {
    let set = sinks.get(key)
    if (!set) sinks.set(key, (set = new Set()))
    set.add(notify)
    return () => {
      const current = sinks.get(key)
      if (!current) return
      current.delete(notify)
      if (current.size === 0) sinks.delete(key)
    }
  }
  const router = createRouter({
    notify: (key) => {
      for (const notify of sinks.get(key) ?? []) notify()
    },
  })
  const instances = new Map<string, Entry>()
  const inflight = new Map<string, Promise<Entry>>()

  // ── refcount tokens/leases ────────────────────────────────────────

  function sweep(entry: Entry): void {
    if (entry.tokens + entry.leases === 0) entry.dispose()
  }

  function mintToken(entry: Entry, identityKey: string, seqAtRead: number, notify: () => void): ReadToken {
    entry.tokens++
    // Seam 1 (db.live serialize-time activation): an un-redeemed token joins NO sink — it is INERT.
    // The subscription is made at REDEEM (activation), when the channel that consumes `notify` exists.
    // Any invalidation during the read window (mint→redeem) would fire into a not-yet-wired channel;
    // the redeem fence (seam 2) replays it exactly once against the just-wired channel, so nothing is
    // lost and nothing fires prematurely.
    let phase: 'open' | 'redeemed' | 'released' = 'open'
    return {
      instanceKey: identityKey,
      seqAtRead,
      redeem() {
        assertUsage(phase === 'open', 'read token already redeemed or released')
        phase = 'redeemed'
        entry.leases++ // add the lease BEFORE dropping the token — refcount never dips to zero
        entry.tokens--
        const unsubscribe = subscribe(identityKey, notify) // seam 1: join the sink AT activation
        // Seam 2 — the σ-read→activation fence: a change landed between the σ-read (seqAtRead, captured
        // at mint) and this activation, so the just-wired channel would miss it. Replay exactly once.
        if (entry.graph.invalidationSeq() > seqAtRead) notify()
        let released = false
        return {
          release() {
            if (released) return
            released = true
            unsubscribe() // this subscriber leaves; other subscribers of the shared graph stay notified
            entry.leases--
            sweep(entry)
          },
        }
      },
      release() {
        if (phase === 'released') return // idempotent
        assertUsage(phase === 'open', 'read token already redeemed')
        phase = 'released'
        // Un-redeemed release: the token never joined a sink (seam 1), so there is nothing to
        // unsubscribe — drop the ref and sweep (net-zero: the eagerly-hydrated graph is disposed).
        entry.tokens--
        sweep(entry)
      },
    }
  }

  function attach(entry: Entry, request: AcquireRequest): AcquireResult {
    const token = mintToken(entry, request.instanceKey, entry.graph.invalidationSeq(), request.notify)
    return { graph: entry.graph, token }
  }

  // ── instance construction ─────────────────────────────────────────

  function buildInstance(plan: GraphPlan, request: AcquireRequest): Entry {
    let graph!: LiveGraph
    let entry!: Entry
    const routable: RoutableGraph = {
      tables: request.tables,
      apply: (changes) => graph.apply(changes),
      notifyKey: () => request.instanceKey,
      fault: () => graph.fault(),
      reseed: () => graph.reseed(),
    }
    // Register inputs with the router BEFORE the graph's seed reads (activate-before-read):
    // no event window can slip between the read and registration.
    router.register(routable)
    graph = createLiveGraph(specOf(plan, request, config.maxStateRowsPerInput))
    entry = {
      graph,
      routable,
      tokens: 0,
      leases: 0,
      // Refcount 0 → tear down exactly once (the last token/lease releases once each, so this fires
      // once): free graph state so a late seed completion is inert, drop the router registration and
      // the identity's maps so a later acquire recompiles a fresh graph.
      dispose: () => {
        graph.destroy()
        router.unregister(routable)
        instances.delete(request.instanceKey)
        sinks.delete(request.instanceKey)
      },
    }
    return entry
  }

  return {
    async acquire(request) {
      const existing = instances.get(request.instanceKey)
      if (existing) {
        // Blocks until the CURRENT seed cycle lands. That is the initial seed for a graph still warming,
        // and a RESEED for one rebuilding its baseline after an image-less event — `ready()` is re-armed
        // per cycle, so this cannot return early on a promise the previous cycle resolved.
        await existing.graph.ready()
        return attach(existing, request)
      }

      const pending = inflight.get(request.instanceKey)
      if (pending) return attach(await pending, request)

      const creation = (async () => {
        try {
          const plan = await request.compilePlan()
          const entry = buildInstance(plan, request)
          instances.set(request.instanceKey, entry)
          await entry.graph.ready() // block acquire on the seed → a precise (live) or demoted (coarse) graph
          inflight.delete(request.instanceKey)
          return entry
        } catch (error) {
          inflight.delete(request.instanceKey)
          throw error // no cache write on failure → no poisoned entry; a later acquire retries
        }
      })()
      inflight.set(request.instanceKey, creation)
      return attach(await creation, request)
    },
    router,
  }
}

/** The live-graph spec for a compiled plan: coarse plans and RLS-gated stateful plans are
 *  born coarse; stateless plans are born live; other stateful plans seed. */
function specOf(plan: GraphPlan, request: AcquireRequest, maxStateRows: number): Parameters<typeof createLiveGraph>[0] {
  const base = { instanceKey: request.instanceKey, tables: request.tables }
  if (plan.coarse) return { kind: 'coarse', ...base }
  if (plan.stateless) return { kind: 'stateless', ...base, instantiate: () => plan.instantiate() }
  return {
    kind: 'stateful',
    ...base,
    instantiate: () => plan.instantiate() as StatefulGraph,
    executor: request.executor,
    maxStateRows,
    bornCoarse: request.rlsEnabled, // rls / 'unknown' → born coarse (never hydrates row state)
  }
}
