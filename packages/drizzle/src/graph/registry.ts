// The graph registry (final-plan §5.5; T5.A/D/F): two-level identity, leases/tokens, caps and
// sentinels. Compiled factories are cached by PlanKey (bounded, evicted); graph STATE is keyed
// by InstanceKey. `acquire` dedups on the canonical instance string; two concurrent acquires of
// a not-yet-created identity share ONE in-flight creation, and a failed creation clears the
// entry so a later acquire retries (no poisoned cache). Refcount = channel leases + unredeemed
// read tokens; a read token holds the initial-read fence and REDEEMS into a lease by atomic
// transfer (no transient zero); refcount 0 destroys immediately (no grace). Capacity is
// reserved BEFORE the async compile, so beyond `maxGraphs` new identities deterministically
// attach to shared per-table coarse sentinels rather than racing into unbounded full graphs.

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
  /** Compile this query to a plan; run AT MOST ONCE per PlanKey (factory cache). */
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

type AcquireResult = { graph: LiveGraph; token: ReadToken; sentinel: boolean }

type Registry = {
  acquire(request: AcquireRequest): Promise<AcquireResult>
  /** Relation drift (epoch change): retire every instance of a plan (destroy + one coarse
   *  invalidation each) and evict its factory, so the next read recompiles under the new epoch. */
  retirePlan(planKey: string): void
  readonly router: Router
  inspect(): { graphs: number; sentinels: number; factories: number; sentinelActivations: number }
}

type Entry = {
  kind: 'instance' | 'sentinel'
  planKey: string
  graph: LiveGraph
  routable: RoutableGraph
  notifyKeys: Set<string>
  tokens: number
  leases: number
  /** Set the instant this entry's teardown is owned (refcount-zero dispose OR drift retire), so
   *  a later token/lease release on the SAME entry is inert — capacity is freed exactly once. */
  dead: boolean
  dispose: () => void
}

function createRegistry(config: {
  maxGraphs: number
  maxStateRowsPerInput: number
  factoryCacheLimit: number
}): Registry {
  const sinks = new Map<string, () => void>()
  const router = createRouter({ notify: (key) => sinks.get(key)?.() })
  const factories = new Map<string, GraphPlan>()
  const instances = new Map<string, Entry>()
  const sentinels = new Map<string, Entry>()
  const inflight = new Map<string, Promise<Entry>>()
  let activeCount = 0
  let sentinelActivations = 0

  // ── factory cache (bounded, LRU) ──────────────────────────────────

  async function compileCached(planKey: string, compile: AcquireRequest['compilePlan']): Promise<GraphPlan> {
    const hit = factories.get(planKey)
    if (hit) {
      factories.delete(planKey)
      factories.set(planKey, hit) // touch → most-recent
      return hit
    }
    const plan = await Promise.resolve(compile())
    factories.set(planKey, plan)
    if (factories.size > config.factoryCacheLimit) factories.delete(factories.keys().next().value!)
    return plan
  }

  // ── refcount tokens/leases ────────────────────────────────────────

  function sweep(entry: Entry, identityKey: string): void {
    if (entry.dead) return // teardown already owned (retire/destroy) — a residual release is inert
    if (entry.kind === 'sentinel') entry.notifyKeys.delete(identityKey)
    if (entry.tokens + entry.leases === 0) entry.dispose()
  }

  /** Retire an instance entry's registry accounting EXACTLY ONCE: drop the router registration,
   *  the identity maps, and its `maxGraphs` slot. Idempotent via `entry.dead`, so whichever path
   *  fires first — a refcount-zero dispose or a drift `retirePlan` — owns the single `activeCount`
   *  decrement, and any still-open token releasing afterward can never decrement it again
   *  (T5.A5/D3/F3: capacity can't be pushed below the true live count). */
  function finalizeInstance(entry: Entry, identityKey: string): void {
    if (entry.dead) return
    entry.dead = true
    router.unregister(entry.routable)
    instances.delete(identityKey)
    sinks.delete(identityKey)
    activeCount--
  }

  function mintToken(entries: Entry[], identityKey: string, seqAtRead: number): ReadToken {
    for (const entry of entries) entry.tokens++
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
            for (const entry of entries) {
              entry.leases--
              sweep(entry, identityKey)
            }
          },
        }
      },
      release() {
        if (phase === 'released') return // idempotent
        assertUsage(phase === 'open', 'read token already redeemed')
        phase = 'released'
        for (const entry of entries) {
          entry.tokens--
          sweep(entry, identityKey)
        }
      },
    }
  }

  function attach(entry: Entry, request: AcquireRequest): AcquireResult {
    sinks.set(request.instanceKey, request.notify)
    const token = mintToken([entry], request.instanceKey, entry.graph.invalidationSeq())
    return { graph: entry.graph, token, sentinel: entry.kind === 'sentinel' }
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
      resync: () => graph.rewarm(),
    }
    // Register inputs with the router BEFORE the graph's warming scan reads (activate-before-read,
    // T5.A3): no event window can slip between the read and registration.
    router.register(routable)
    graph = createLiveGraph(specOf(plan, request, config.maxStateRowsPerInput))
    entry = {
      kind: 'instance',
      planKey: request.planKey,
      graph,
      routable,
      notifyKeys,
      tokens: 0,
      leases: 0,
      dead: false,
      dispose: () => {
        if (entry.dead) return // already finalized (e.g. by retirePlan) — this late release is inert
        graph.destroy() // free state + abort any in-flight warming so a late completion is inert (T5.A5/C7)
        finalizeInstance(entry, request.instanceKey)
      },
    }
    return entry
  }

  function acquireSentinel(request: AcquireRequest): AcquireResult {
    const entries: Entry[] = []
    for (const table of request.tables) entries.push(ensureSentinel(table))
    for (const entry of entries) entry.notifyKeys.add(request.instanceKey)
    sinks.set(request.instanceKey, request.notify)
    const seqAtRead = entries[0] ? entries[0].graph.invalidationSeq() : 0
    return { graph: entries[0]!.graph, token: mintToken(entries, request.instanceKey, seqAtRead), sentinel: true }
  }

  function ensureSentinel(table: string): Entry {
    const existing = sentinels.get(table)
    if (existing) return existing
    const notifyKeys = new Set<string>()
    const dispose = (): void => {
      if (entry.dead) return
      entry.dead = true
      graph.destroy()
      router.unregister(entry.routable)
      sentinels.delete(table)
    }
    const graph = createLiveGraph({
      kind: 'coarse',
      instanceKey: `sentinel:${table}`,
      tables: [table],
      reason: 'sentinel',
    })
    const routable: RoutableGraph = {
      tables: [table],
      apply: (changes) => graph.apply(changes),
      notifyKeys: () => notifyKeys,
      resync: () => {},
    }
    const entry: Entry = {
      kind: 'sentinel',
      planKey: `sentinel:${table}`,
      graph,
      routable,
      notifyKeys,
      tokens: 0,
      leases: 0,
      dead: false,
      dispose,
    }
    sentinels.set(table, entry)
    router.register(routable)
    sentinelActivations++
    return entry
  }

  return {
    async acquire(request) {
      const existing = instances.get(request.instanceKey)
      if (existing) return attach(existing, request)

      const pending = inflight.get(request.instanceKey)
      if (pending) return attach(await pending, request)

      // Reserve capacity synchronously, BEFORE the async compile, so the N+1th concurrent
      // unique creation deterministically gets a sentinel even while compiles race (T5.F3).
      if (activeCount >= config.maxGraphs) return acquireSentinel(request)
      activeCount++

      const creation = (async () => {
        try {
          const plan = await compileCached(request.planKey, request.compilePlan)
          const entry = buildInstance(plan, request)
          instances.set(request.instanceKey, entry)
          inflight.delete(request.instanceKey)
          return entry
        } catch (error) {
          inflight.delete(request.instanceKey)
          activeCount-- // release the reservation — no poisoned cache entry
          throw error
        }
      })()
      inflight.set(request.instanceKey, creation)
      return attach(await creation, request)
    },
    retirePlan(planKey) {
      factories.delete(planKey)
      for (const [key, entry] of [...instances]) {
        if (entry.planKey !== planKey) continue
        entry.graph.retire() // → 'retired' (terminal): frees state, aborts any in-flight warming
        sinks.get(key)?.() // exactly one coarse invalidation to the identity, BEFORE finalize drops the sink
        finalizeInstance(entry, key) // single-owner teardown: a residual token release stays inert
      }
    },
    router,
    inspect: () => ({
      graphs: instances.size,
      sentinels: sentinels.size,
      factories: factories.size,
      sentinelActivations,
    }),
  }
}

/** The live-graph spec for a compiled plan: coarse plans and RLS-gated stateful plans are
 *  born coarse; stateless plans are born live; other stateful plans warm. */
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
