// The graph registry (final-plan §5.5; T5.A/D): two-level identity + leases/tokens. A PlanKey's
// compiled plan is cached while ≥1 InstanceKey references it (reference-scoped — dropped when the
// last instance of that plan disposes; a later re-acquire recompiles); graph STATE is keyed by
// InstanceKey. `acquire` dedups on the canonical instance string; two concurrent acquires of a
// not-yet-created identity share ONE in-flight creation, and a failed creation clears the entry so
// a later acquire retries (no poisoned cache). Refcount = channel leases + unredeemed read tokens;
// a read token holds the initial-read fence and REDEEMS into a lease by atomic transfer (no
// transient zero); refcount 0 destroys immediately and synchronously (no grace, no cap, no pool).

export { type Registry, type AcquireRequest, type AcquireResult, type ReadToken, type Lease, createRegistry }

import type { GraphPlan, StatefulGraph } from '../compile/compile.js'
import { assertUsage } from '../utils/assert.js'
import type { HydrationExecutor } from './hydrate.js'
import { type LiveGraph, createLiveGraph } from './liveGraph.js'
import { type RoutableGraph, type Router, createRouter } from '../router/changeRouter.js'

type AcquireRequest = {
  planKey: string
  instanceKey: string
  tables: string[]
  rlsEnabled: boolean
  /** Compile this query to a plan; run once per PlanKey while it is referenced (reference-scoped
   *  cache), recompiled if re-acquired after the last instance of the plan disposed. */
  compilePlan: () => GraphPlan | Promise<GraphPlan>
  executor: HydrationExecutor
  /** The identity's invalidation sink (ticket 6 wires the channel; ticket 5 observes it). */
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
  /** Relation drift (epoch change): retire every instance of a plan (destroy + one coarse
   *  invalidation each) and drop its cached plan, so the next read recompiles under the new epoch. */
  retirePlan(planKey: string): void
  readonly router: Router
  inspect(): { graphs: number; factories: number }
}

type Entry = {
  planKey: string
  graph: LiveGraph
  routable: RoutableGraph
  notifyKeys: Set<string>
  tokens: number
  leases: number
  /** Set the instant this entry's teardown is owned (refcount-zero dispose OR drift retire), so a
   *  later token/lease release on the SAME entry is inert — the graph is torn down exactly once. */
  dead: boolean
  dispose: () => void
}

function createRegistry(config: { maxStateRowsPerInput: number }): Registry {
  // Per-identity subscriber sets: several acquires of the SAME instanceKey are distinct live
  // subscribers of ONE shared graph (independent channels), so an invalidation must notify EVERY
  // one — a single callback per key would silence all but the last. A subscriber joins when its read
  // token is minted and leaves on that token/lease's final release; refcount is unchanged (leases +
  // tokens), so the last subscriber out still disposes the graph.
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
  // Reference-scoped plan cache: the compiled plan for a PlanKey, held while ≥1 instance references
  // it (`planRefs` counts them). No size cap, no LRU — the last instance of a plan drops it.
  const factories = new Map<string, GraphPlan>()
  const planRefs = new Map<string, number>()
  const instances = new Map<string, Entry>()
  const inflight = new Map<string, Promise<Entry>>()

  // ── reference-scoped plan cache ───────────────────────────────────

  async function planFor(planKey: string, compile: AcquireRequest['compilePlan']): Promise<GraphPlan> {
    const hit = factories.get(planKey)
    return hit ?? Promise.resolve(compile())
  }

  function retainPlan(planKey: string, plan: GraphPlan): void {
    factories.set(planKey, plan)
    planRefs.set(planKey, (planRefs.get(planKey) ?? 0) + 1)
  }

  function releasePlan(planKey: string): void {
    const remaining = (planRefs.get(planKey) ?? 1) - 1
    if (remaining <= 0) {
      planRefs.delete(planKey)
      factories.delete(planKey) // last instance of this plan gone → drop the cached plan
    } else {
      planRefs.set(planKey, remaining)
    }
  }

  // ── refcount tokens/leases ────────────────────────────────────────

  function sweep(entry: Entry): void {
    if (entry.dead) return // teardown already owned (retire/destroy) — a residual release is inert
    if (entry.tokens + entry.leases === 0) entry.dispose()
  }

  /** Tear an instance entry down EXACTLY ONCE: drop the router registration, the identity maps, and
   *  its plan-cache reference. Idempotent via `entry.dead`, so whichever path fires first — a
   *  refcount-zero dispose or a drift `retirePlan` — owns the teardown, and any still-open token
   *  releasing afterward is inert (T5.A5/D3). */
  function finalizeInstance(entry: Entry, identityKey: string): void {
    if (entry.dead) return
    entry.dead = true
    router.unregister(entry.routable)
    instances.delete(identityKey)
    sinks.delete(identityKey)
    releasePlan(entry.planKey)
  }

  function mintToken(entries: Entry[], identityKey: string, seqAtRead: number, notify: () => void): ReadToken {
    for (const entry of entries) entry.tokens++
    // Join the subscriber set at MINT (not redeem): an unredeemed read token whose plan drifts
    // (retirePlan) must still be told to re-read (T5.D3).
    const unsubscribe = subscribe(identityKey, notify)
    let phase: 'open' | 'redeemed' | 'released' = 'open'
    return {
      instanceKey: identityKey,
      seqAtRead,
      redeem() {
        assertUsage(phase === 'open', 'read token already redeemed or released')
        for (const entry of entries) {
          entry.leases++ // add the lease BEFORE dropping the token — refcount never dips to zero
          entry.tokens--
        }
        phase = 'redeemed'
        let released = false
        return {
          release() {
            if (released) return
            released = true
            unsubscribe() // this subscriber leaves; other subscribers of the shared graph stay notified
            for (const entry of entries) {
              entry.leases--
              sweep(entry)
            }
          },
        }
      },
      release() {
        if (phase === 'released') return // idempotent
        assertUsage(phase === 'open', 'read token already redeemed')
        phase = 'released'
        unsubscribe()
        for (const entry of entries) {
          entry.tokens--
          sweep(entry)
        }
      },
    }
  }

  function attach(entry: Entry, request: AcquireRequest): AcquireResult {
    const token = mintToken([entry], request.instanceKey, entry.graph.invalidationSeq(), request.notify)
    return { graph: entry.graph, token }
  }

  // ── instance construction ─────────────────────────────────────────

  function buildInstance(plan: GraphPlan, request: AcquireRequest): Entry {
    const notifyKeys = new Set([request.instanceKey])
    let graph!: LiveGraph
    let entry!: Entry
    const routable: RoutableGraph = {
      tables: request.tables,
      apply: (changes) => graph.apply(changes),
      notifyKeys: () => notifyKeys,
      fault: () => graph.fault(),
    }
    // Register inputs with the router BEFORE the graph's seed reads (activate-before-read, T5.A3):
    // no event window can slip between the read and registration.
    router.register(routable)
    graph = createLiveGraph(specOf(plan, request, config.maxStateRowsPerInput))
    entry = {
      planKey: request.planKey,
      graph,
      routable,
      notifyKeys,
      tokens: 0,
      leases: 0,
      dead: false,
      dispose: () => {
        if (entry.dead) return // already finalized (e.g. by retirePlan) — this late release is inert
        graph.destroy() // free state so a late completion is inert (T5.A5/C7)
        finalizeInstance(entry, request.instanceKey)
      },
    }
    return entry
  }

  return {
    async acquire(request) {
      const existing = instances.get(request.instanceKey)
      if (existing) {
        await existing.graph.ready() // a concurrent acquire during the initial seed blocks until precise
        return attach(existing, request)
      }

      const pending = inflight.get(request.instanceKey)
      if (pending) return attach(await pending, request)

      const creation = (async () => {
        try {
          const plan = await planFor(request.planKey, request.compilePlan)
          const entry = buildInstance(plan, request)
          instances.set(request.instanceKey, entry)
          retainPlan(request.planKey, plan) // reference-scoped cache: this instance holds the plan
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
    retirePlan(planKey) {
      factories.delete(planKey)
      planRefs.delete(planKey)
      for (const [key, entry] of [...instances]) {
        if (entry.planKey !== planKey) continue
        entry.graph.retire() // → 'retired' (terminal): frees state
        for (const notify of sinks.get(key) ?? []) notify() // one coarse invalidation per subscriber, BEFORE finalize drops the sink
        finalizeInstance(entry, key) // single-owner teardown: a residual token release stays inert
      }
    },
    router,
    inspect: () => ({ graphs: instances.size, factories: factories.size }),
  }
}

/** The live-graph spec for a compiled plan: coarse plans and RLS-gated stateful plans are
 *  born coarse; stateless plans are born live; other stateful plans seed. */
function specOf(plan: GraphPlan, request: AcquireRequest, maxStateRows: number): Parameters<typeof createLiveGraph>[0] {
  const base = { instanceKey: request.instanceKey, tables: request.tables }
  if (plan.coarse) return { kind: 'coarse', ...base, reason: 'coarse-plan' }
  if (plan.stateless) return { kind: 'stateless', ...base, instantiate: () => plan.instantiate() }
  return {
    kind: 'stateful',
    ...base,
    instantiate: () => plan.instantiate() as StatefulGraph,
    executor: request.executor,
    maxStateRows,
    bornCoarse: request.rlsEnabled ? 'rls' : undefined,
  }
}
