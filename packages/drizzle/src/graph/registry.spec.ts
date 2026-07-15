// T5.A/D/F — the registry: canonical acquire dedup (A1), concurrent-share + failure recovery
// (A2), activate-before-read spy order (A3), born-state by plan class (A4), refcount =
// leases + unredeemed tokens with atomic redeem-transfer + immediate destroy (A5/A7),
// state-row + no-PK born-coarse (A6), two-level identity with a bounded factory cache
// (D1/D2), drift-retire (D3), and per-table coarse sentinels at cap (F1/F2/F3). All fakes are
// deterministic — pending promises drive the async paths, never timers.

import { describe, expect, it, vi } from 'vitest'
import type { FireResult, GraphPlan, SeedDescriptor, StatefulGraph } from '../compile/compile.js'
import type { Row } from '../compile/rowSpace.js'
import type { HydrationExecutor } from './hydrate.js'
import { type AcquireRequest, createRegistry } from './registry.js'

// ── deterministic async plumbing ────────────────────────────────────

type Deferred<T> = { promise: Promise<T>; resolve: (value: T) => void; reject: (error: unknown) => void }
function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

const flush = (): Promise<void> => new Promise((resolve) => setImmediate(resolve))
async function settle(graph: { state(): string }): Promise<void> {
  for (let i = 0; i < 1000 && graph.state() === 'warming'; i++) await flush()
}

const NO_FIRE: FireResult = { data: false, dirty: false, invalidated: false }

function fakeExecutor(over: Partial<HydrationExecutor> = {}): HydrationExecutor {
  return {
    scan: over.scan ?? (() => new Promise<Row[]>(() => {})), // never resolves → stays warming
    fetchByKeys: over.fetchByKeys ?? (() => Promise.resolve([])),
  }
}

function seed(inputId: string, table: string, primaryKey: string[] = ['id']): SeedDescriptor {
  return { inputId, table, alias: inputId, primaryKey, columns: '*', residual: { kind: 'true' }, shadowNeed: true }
}

function statefulFake(seeds: SeedDescriptor[]): StatefulGraph {
  return {
    seeds,
    seedInput() {},
    flushSeed() {},
    feedInput() {},
    runBatch: () => NO_FIRE,
    apply: () => NO_FIRE,
  }
}

const inputsOf = (tables: string[]): GraphPlan['inputs'] =>
  tables.map((table) => ({ alias: table, table, columns: '*' as const, shadowNeed: false }))

function coarsePlan(tables: string[]): GraphPlan {
  return {
    tables,
    stateless: true,
    coarse: true,
    inputs: inputsOf(tables),
    instantiate: () => ({ apply: () => NO_FIRE }),
  }
}
function statelessPlan(tables: string[]): GraphPlan {
  return {
    tables,
    stateless: true,
    coarse: false,
    inputs: inputsOf(tables),
    instantiate: () => ({ apply: () => NO_FIRE }),
  }
}
function statefulPlan(tables: string[], seeds: SeedDescriptor[]): GraphPlan {
  return { tables, stateless: false, coarse: false, inputs: inputsOf(tables), instantiate: () => statefulFake(seeds) }
}

function req(over: Partial<AcquireRequest> & { compilePlan: AcquireRequest['compilePlan'] }): AcquireRequest {
  return {
    planKey: over.planKey ?? 'plan-1',
    instanceKey: over.instanceKey ?? 'inst-1',
    tables: over.tables ?? ['users'],
    rlsEnabled: over.rlsEnabled ?? false,
    compilePlan: over.compilePlan,
    executor: over.executor ?? fakeExecutor(),
    notify: over.notify ?? (() => {}),
  }
}

const registryOf = (over: Partial<Parameters<typeof createRegistry>[0]> = {}) =>
  createRegistry({
    maxGraphs: over.maxGraphs ?? 10,
    maxStateRowsPerInput: over.maxStateRowsPerInput ?? 100,
    factoryCacheLimit: over.factoryCacheLimit ?? 10,
  })

// ── A1 / A2 — dedup + concurrent creation ───────────────────────────

describe('T5.A1 — canonical acquire dedup', () => {
  it('two acquires of the same identity return one shared graph, compiling once', async () => {
    const registry = registryOf()
    const compile = vi.fn(() => coarsePlan(['users']))
    const a = await registry.acquire(req({ compilePlan: compile }))
    const b = await registry.acquire(req({ compilePlan: compile }))
    expect(a.graph).toBe(b.graph)
    expect(compile).toHaveBeenCalledTimes(1)
    expect(registry.inspect().graphs).toBe(1)
  })
})

describe('T5.A2 — concurrent acquire shares one creation + failure recovery', () => {
  it('two concurrent acquires share one in-flight creation and receive the same graph', async () => {
    const registry = registryOf()
    const plan = deferred<GraphPlan>()
    const compile = vi.fn(() => plan.promise)
    const p1 = registry.acquire(req({ compilePlan: compile }))
    const p2 = registry.acquire(req({ compilePlan: compile }))
    plan.resolve(coarsePlan(['users']))
    const [a, b] = await Promise.all([p1, p2])
    expect(compile).toHaveBeenCalledTimes(1)
    expect(a.graph).toBe(b.graph)
    expect(registry.inspect().graphs).toBe(1)
  })

  it('a failed shared creation rejects both waiters, clears the entry, and a later acquire retries (no poison)', async () => {
    const registry = registryOf()
    const first = deferred<GraphPlan>()
    const compile = vi
      .fn<() => Promise<GraphPlan> | GraphPlan>()
      .mockReturnValueOnce(first.promise)
      .mockReturnValue(coarsePlan(['users']))
    const p1 = registry.acquire(req({ compilePlan: compile }))
    const p2 = registry.acquire(req({ compilePlan: compile }))
    first.reject(new Error('boom'))
    await expect(p1).rejects.toThrow('boom')
    await expect(p2).rejects.toThrow('boom')
    expect(registry.inspect().graphs).toBe(0) // entry cleared, capacity released
    const retry = await registry.acquire(req({ compilePlan: compile }))
    expect(retry.graph.state()).toBe('coarse')
    expect(registry.inspect().graphs).toBe(1)
    expect(compile).toHaveBeenCalledTimes(2) // one shared failed creation + one retry
  })
})

// ── A3 — activate before read ───────────────────────────────────────

describe('T5.A3 — inputs registered before the acquiring read', () => {
  it('router.register runs synchronously before the warming scan (spy order)', async () => {
    const registry = registryOf()
    const order: string[] = []
    const orig = registry.router.register.bind(registry.router)
    vi.spyOn(registry.router, 'register').mockImplementation((graph) => {
      order.push('register')
      orig(graph)
    })
    const executor = fakeExecutor({
      scan: () => {
        order.push('scan')
        return new Promise<Row[]>(() => {})
      },
    })
    await registry.acquire(req({ compilePlan: () => statefulPlan(['users'], [seed('users', 'users')]), executor }))
    expect(order).toContain('register')
    expect(order).toContain('scan')
    expect(order.indexOf('register')).toBeLessThan(order.indexOf('scan'))
  })
})

// ── A4 — born state ─────────────────────────────────────────────────

describe('T5.A4 — born-state by plan class', () => {
  it('a stateless plan is born live', async () => {
    const r = await registryOf().acquire(req({ compilePlan: () => statelessPlan(['users']) }))
    expect(r.graph.state()).toBe('live')
  })
  it('a stateful plan is born warming', async () => {
    const r = await registryOf().acquire(req({ compilePlan: () => statefulPlan(['users'], [seed('users', 'users')]) }))
    expect(r.graph.state()).toBe('warming')
  })
})

// ── A5 / A7 — refcount, tokens, leases ──────────────────────────────

describe('T5.A5 — refcount = leases + unredeemed tokens; immediate destroy on zero', () => {
  it('releasing the sole token (no redeem) destroys immediately: entry gone, router index cleared', async () => {
    const registry = registryOf()
    const r = await registry.acquire(req({ compilePlan: () => coarsePlan(['users']) }))
    expect(registry.inspect().graphs).toBe(1)
    expect(registry.router.inspect().graphs).toBe(1)
    r.token.release()
    expect(r.graph.state()).toBe('destroyed')
    expect(registry.inspect().graphs).toBe(0)
    expect(registry.router.inspect().graphs).toBe(0)
  })

  it('token→lease redeem is an atomic transfer with no transient zero (no destroy-then-recreate)', async () => {
    const registry = registryOf()
    const r = await registry.acquire(req({ compilePlan: () => coarsePlan(['users']) }))
    const lease = r.token.redeem()
    expect(r.graph.state()).not.toBe('destroyed') // never dipped to zero during the transfer
    expect(registry.inspect().graphs).toBe(1)
    lease.release()
    expect(r.graph.state()).toBe('destroyed')
    expect(registry.inspect().graphs).toBe(0)
  })
})

describe('T5.A7 — ReadToken redeem-transfer + disposal', () => {
  it('carries the initial-read fence; a double redeem is rejected; a redeemed lease releases once (idempotent)', async () => {
    const registry = registryOf()
    const r = await registry.acquire(req({ instanceKey: 'inst-x', compilePlan: () => coarsePlan(['users']) }))
    expect(r.token.instanceKey).toBe('inst-x')
    expect(r.token.seqAtRead).toBe(0)
    const lease = r.token.redeem()
    expect(() => r.token.redeem()).toThrow() // double redeem rejected
    lease.release()
    expect(r.graph.state()).toBe('destroyed')
    lease.release() // idempotent — no resurrection
    expect(registry.inspect().graphs).toBe(0)
  })

  it('read-error disposal releases an unredeemed token exactly once (refcount stays correct)', async () => {
    const registry = registryOf()
    const r = await registry.acquire(req({ compilePlan: () => coarsePlan(['users']) }))
    r.token.release() // the initial read failed → dispose the unredeemed token
    expect(r.graph.state()).toBe('destroyed')
    r.token.release() // idempotent second dispose
    expect(registry.inspect().graphs).toBe(0)
  })
})

// ── A6 — state-row bound + no-PK born coarse ────────────────────────

describe('T5.A6 — state-row bound → demote; no-PK input born coarse', () => {
  it('a no-PK stateful input is born coarse (cannot shadow-resolve a retraction)', async () => {
    const r = await registryOf().acquire(
      req({ compilePlan: () => statefulPlan(['users'], [seed('users', 'users', [])]) }),
    )
    expect(r.graph.state()).toBe('coarse')
  })

  it('exceeding maxStateRowsPerInput during warming demotes to coarse and stays coarse (no rewarm loop)', async () => {
    const registry = registryOf({ maxStateRowsPerInput: 2 })
    const executor = fakeExecutor({ scan: () => Promise.resolve([{ id: 1 }, { id: 2 }, { id: 3 }]) })
    const r = await registry.acquire(
      req({ compilePlan: () => statefulPlan(['users'], [seed('users', 'users')]), executor }),
    )
    await settle(r.graph)
    expect(r.graph.state()).toBe('coarse')
    r.graph.rewarm() // coarse is a sink — a resync request does NOT rewarm it
    expect(r.graph.state()).toBe('coarse')
  })
})

// ── D1 / D2 — two-level identity + bounded factory cache ────────────

describe('T5.D1 — PlanKey factory cache (bounded) vs InstanceKey state', () => {
  it('two instances of one plan share the compiled factory but hold independent state', async () => {
    const registry = registryOf()
    const compile = vi.fn(() => coarsePlan(['users']))
    const a = await registry.acquire(req({ planKey: 'P', instanceKey: 'I1', compilePlan: compile }))
    const b = await registry.acquire(req({ planKey: 'P', instanceKey: 'I2', compilePlan: compile }))
    expect(compile).toHaveBeenCalledTimes(1)
    expect(a.graph).not.toBe(b.graph)
    expect(registry.inspect().graphs).toBe(2)
  })

  it('the factory cache is bounded/evicted; re-acquiring an evicted plan recompiles', async () => {
    const registry = registryOf({ factoryCacheLimit: 2 })
    const compileA2 = vi.fn(() => coarsePlan(['users']))
    await registry.acquire(req({ planKey: 'A', instanceKey: 'A', compilePlan: () => coarsePlan(['users']) }))
    await registry.acquire(req({ planKey: 'B', instanceKey: 'B', compilePlan: () => coarsePlan(['users']) }))
    await registry.acquire(req({ planKey: 'C', instanceKey: 'C', compilePlan: () => coarsePlan(['users']) })) // evicts A (LRU)
    expect(registry.inspect().factories).toBe(2)
    await registry.acquire(req({ planKey: 'A', instanceKey: 'A2', compilePlan: compileA2 }))
    expect(compileA2).toHaveBeenCalledTimes(1) // A's factory was evicted → recompiled
  })
})

describe('T5.D2 — schema epoch in PlanKey', () => {
  it('a drifted fingerprint (distinct PlanKey) does not share the factory', async () => {
    const registry = registryOf()
    const e1 = vi.fn(() => coarsePlan(['users']))
    const e2 = vi.fn(() => coarsePlan(['users']))
    await registry.acquire(req({ planKey: 'q@epoch1', instanceKey: 'I1', compilePlan: e1 }))
    await registry.acquire(req({ planKey: 'q@epoch2', instanceKey: 'I2', compilePlan: e2 }))
    expect(e1).toHaveBeenCalledTimes(1)
    expect(e2).toHaveBeenCalledTimes(1)
    expect(registry.inspect().factories).toBe(2)
  })
})

// ── D3 — drift retires ──────────────────────────────────────────────

describe('T5.D3 — drift retires (destroy + one coarse invalidation + recompile-on-next-read)', () => {
  it('retirePlan destroys the instance, fires exactly one coarse invalidation, evicts the factory, and recompiles under the new epoch', async () => {
    const registry = registryOf()
    const notify = vi.fn()
    const r = await registry.acquire(
      req({ planKey: 'q@e1', instanceKey: 'I1', compilePlan: () => coarsePlan(['users']), notify }),
    )
    registry.retirePlan('q@e1')
    expect(r.graph.state()).toBe('retired')
    expect(notify).toHaveBeenCalledTimes(1)
    expect(registry.inspect().graphs).toBe(0)
    expect(registry.inspect().factories).toBe(0)
    const compileNew = vi.fn(() => coarsePlan(['users']))
    const r2 = await registry.acquire(req({ planKey: 'q@e2', instanceKey: 'I2', compilePlan: compileNew }))
    expect(compileNew).toHaveBeenCalledTimes(1)
    expect(r2.graph.state()).toBe('coarse')
  })

  it('retire transfers teardown ownership: a still-open token released after retire is inert, so maxGraphs stays binding (no capacity double-free)', async () => {
    const registry = registryOf({ maxGraphs: 1 })
    const r = await registry.acquire(req({ planKey: 'p', instanceKey: 'I0', compilePlan: () => coarsePlan(['users']) }))
    registry.retirePlan('p') // owns the single capacity slot's teardown: activeCount 1 → 0
    r.token.release() // the token was still open at retire; its late release MUST NOT decrement again

    // Under maxGraphs=1 exactly one of the next two identities may be a full graph; the other is a
    // sentinel. With the retire/release double-decrement, activeCount would be -1 and BOTH would be
    // full graphs — the cap silently stops binding.
    const a = await registry.acquire(req({ planKey: 'a', instanceKey: 'I1', compilePlan: () => coarsePlan(['users']) }))
    const b = await registry.acquire(req({ planKey: 'b', instanceKey: 'I2', compilePlan: () => coarsePlan(['users']) }))
    expect([a.sentinel, b.sentinel].filter((s) => !s)).toHaveLength(1)
    expect(a.sentinel).toBe(false)
    expect(b.sentinel).toBe(true)
    expect(registry.inspect().graphs).toBe(1) // only `a` is a live full graph; the retired `p` is gone
  })
})

// ── F1 / F2 / F3 — sentinels at cap ─────────────────────────────────

describe('T5.F1 — maxGraphs → per-table coarse sentinels', () => {
  it('beyond the cap, identities attach to a shared per-table sentinel that coarse-invalidates every attached identity', async () => {
    const registry = registryOf({ maxGraphs: 1 })
    const n2 = vi.fn()
    const n3 = vi.fn()
    const r1 = await registry.acquire(
      req({ planKey: 'A', instanceKey: 'I1', compilePlan: () => coarsePlan(['users']) }),
    )
    const r2 = await registry.acquire(
      req({ planKey: 'B', instanceKey: 'I2', compilePlan: () => coarsePlan(['users']), notify: n2 }),
    )
    const r3 = await registry.acquire(
      req({ planKey: 'C', instanceKey: 'I3', compilePlan: () => coarsePlan(['users']), notify: n3 }),
    )
    expect(r1.sentinel).toBe(false)
    expect(r2.sentinel).toBe(true)
    expect(r3.sentinel).toBe(true)
    expect(r2.graph).toBe(r3.graph) // shared per-table sentinel
    registry.router.ingest({
      sourceId: 's',
      position: 1,
      predecessor: null,
      changes: [{ table: 'users', kind: 'insert', new: { id: 1 } }],
    })
    expect(n2).toHaveBeenCalledTimes(1)
    expect(n3).toHaveBeenCalledTimes(1)
  })
})

describe('T5.F2 — sentinel zero-ref eviction', () => {
  it('a sentinel with zero attached refs is evicted; a later identity re-creates it', async () => {
    const registry = registryOf({ maxGraphs: 1 })
    await registry.acquire(req({ planKey: 'A', instanceKey: 'I1', compilePlan: () => coarsePlan(['users']) }))
    const s = await registry.acquire(req({ planKey: 'B', instanceKey: 'I2', compilePlan: () => coarsePlan(['users']) }))
    expect(s.sentinel).toBe(true)
    expect(registry.inspect().sentinels).toBe(1)
    s.token.release()
    expect(registry.inspect().sentinels).toBe(0)
    const s2 = await registry.acquire(
      req({ planKey: 'C', instanceKey: 'I3', compilePlan: () => coarsePlan(['users']) }),
    )
    expect(s2.sentinel).toBe(true)
    expect(registry.inspect().sentinels).toBe(1)
  })
})

describe('T5.F3 — capacity reserved before the async compile', () => {
  it('N creations at the cap + an N+1th racing creation: the N+1th deterministically gets a sentinel', async () => {
    const registry = registryOf({ maxGraphs: 2 })
    const plan = deferred<GraphPlan>()
    const compile = () => plan.promise // all compiles race (pending) while capacity is reserved
    const p1 = registry.acquire(req({ planKey: 'A', instanceKey: 'I1', compilePlan: compile }))
    const p2 = registry.acquire(req({ planKey: 'B', instanceKey: 'I2', compilePlan: compile }))
    const p3 = registry.acquire(req({ planKey: 'C', instanceKey: 'I3', compilePlan: compile }))
    plan.resolve(coarsePlan(['users']))
    const [r1, r2, r3] = await Promise.all([p1, p2, p3])
    expect(r1.sentinel).toBe(false)
    expect(r2.sentinel).toBe(false)
    expect(r3.sentinel).toBe(true)
  })
})
