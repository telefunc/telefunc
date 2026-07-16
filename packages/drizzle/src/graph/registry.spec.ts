// T5.A/D + §6 reopen — the registry: canonical acquire dedup (A1), concurrent-share + failure
// recovery (A2), activate-before-read spy order (A3), born-state by plan class (A4), refcount =
// leases + unredeemed tokens with atomic redeem-transfer + immediate destroy (A5/A7), state-row +
// no-PK born-coarse (A6), two-level identity with a reference-scoped plan cache (D1/D2),
// drift-retire (D3), and the db.live registry reopen (§6): subscribe-at-redeem (un-redeemed tokens
// inert), the seqAtRead activation fence, and the dead-entry catch-up. All fakes are deterministic —
// pending promises drive the async paths, never timers.

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

const NO_FIRE: FireResult = { data: false, dirty: false, invalidated: false }

function fakeExecutor(over: Partial<HydrationExecutor> = {}): HydrationExecutor {
  // acquire BLOCKS on the seed, so the scan must resolve for the graph to reach live.
  return { scan: over.scan ?? (() => Promise.resolve<Row[]>([])) }
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

function coarsePlan(tables: string[]): GraphPlan {
  return {
    tables,
    stateless: true,
    coarse: true,
    instantiate: () => ({ apply: () => NO_FIRE }),
  }
}
function statelessPlan(tables: string[]): GraphPlan {
  return {
    tables,
    stateless: true,
    coarse: false,
    instantiate: () => ({ apply: () => NO_FIRE }),
  }
}
function statefulPlan(tables: string[], seeds: SeedDescriptor[]): GraphPlan {
  return { tables, stateless: false, coarse: false, instantiate: () => statefulFake(seeds) }
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
  createRegistry({ maxStateRowsPerInput: over.maxStateRowsPerInput ?? 100 })

// ── BLOCKER #5a — multi-subscriber notify ownership ─────────────────

describe('registry — multiple subscribers to one shared graph must all be notified', () => {
  it('a second acquire of the identical query overwrites (silences) the first subscriber (dropped notify)', async () => {
    const registry = registryOf()
    const n1 = vi.fn()
    const n2 = vi.fn()
    const a = await registry.acquire(
      req({ instanceKey: 'shared', compilePlan: () => coarsePlan(['users']), notify: n1 }),
    )
    const b = await registry.acquire(
      req({ instanceKey: 'shared', compilePlan: () => coarsePlan(['users']), notify: n2 }),
    )
    expect(a.graph).toBe(b.graph) // deduped to one shared graph — both callers subscribe to it
    a.token.redeem() // both clients are live subscribers holding leases
    b.token.redeem()
    registry.router.ingest({ changes: [{ table: 'users', kind: 'insert', new: { id: 1 } }] })
    expect(n2).toHaveBeenCalledTimes(1) // the second subscriber hears the invalidation
    expect(n1).toHaveBeenCalledTimes(1) // and so does the first — `sinks` holds a SET per instanceKey, not one callback
  })
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
  it('router.register runs synchronously before the seed scan (spy order)', async () => {
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
        return Promise.resolve<Row[]>([])
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
  it('a stateful plan is born LIVE after the synchronous seed (acquire blocks on it)', async () => {
    const r = await registryOf().acquire(req({ compilePlan: () => statefulPlan(['users'], [seed('users', 'users')]) }))
    expect(r.graph.state()).toBe('live')
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

  it('a seed exceeding maxStateRowsPerInput demotes to coarse', async () => {
    const registry = registryOf({ maxStateRowsPerInput: 2 })
    const executor = fakeExecutor({ scan: () => Promise.resolve([{ id: 1 }, { id: 2 }, { id: 3 }]) })
    const r = await registry.acquire(
      req({ compilePlan: () => statefulPlan(['users'], [seed('users', 'users')]), executor }),
    )
    expect(r.graph.state()).toBe('coarse') // acquire blocked on the seed, which demoted over the bound
  })
})

// ── D1 / D2 — two-level identity + reference-scoped plan cache ──────

describe('T5.D1 — PlanKey plan cache vs InstanceKey state', () => {
  it('two instances of one plan share the compiled plan but hold independent state', async () => {
    const registry = registryOf()
    const compile = vi.fn(() => coarsePlan(['users']))
    const a = await registry.acquire(req({ planKey: 'P', instanceKey: 'I1', compilePlan: compile }))
    const b = await registry.acquire(req({ planKey: 'P', instanceKey: 'I2', compilePlan: compile }))
    expect(compile).toHaveBeenCalledTimes(1)
    expect(a.graph).not.toBe(b.graph)
    expect(registry.inspect().graphs).toBe(2)
  })

  it('the plan cache is reference-scoped: dropped when the last instance disposes, recompiled on re-acquire', async () => {
    const registry = registryOf()
    const compile = vi.fn(() => coarsePlan(['users']))
    const a = await registry.acquire(req({ planKey: 'P', instanceKey: 'I1', compilePlan: compile }))
    const b = await registry.acquire(req({ planKey: 'P', instanceKey: 'I2', compilePlan: compile }))
    expect(compile).toHaveBeenCalledTimes(1)
    expect(registry.inspect().factories).toBe(1)
    a.token.release() // one instance still references the plan → plan retained
    expect(registry.inspect().factories).toBe(1)
    b.token.release() // last instance gone → plan dropped
    expect(registry.inspect().factories).toBe(0)
    await registry.acquire(req({ planKey: 'P', instanceKey: 'I3', compilePlan: compile }))
    expect(compile).toHaveBeenCalledTimes(2) // re-acquire after the last dispose recompiles
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

describe('T5.D3 — drift retires (destroy + one coarse invalidation to wired subscribers + recompile-on-next-read)', () => {
  it('retirePlan destroys the instance, fires one coarse invalidation to a redeemed subscriber, evicts the factory, and recompiles under the new epoch', async () => {
    const registry = registryOf()
    const notify = vi.fn()
    const r = await registry.acquire(
      req({ planKey: 'q@e1', instanceKey: 'I1', compilePlan: () => coarsePlan(['users']), notify }),
    )
    r.token.redeem() // a wired subscriber (leased) — retire must coarse-notify it (subscribe-at-redeem)
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

  it('teardown-once: a redeemed lease released after retire is inert (no double teardown, no re-notify)', async () => {
    const registry = registryOf()
    const notify = vi.fn()
    const r = await registry.acquire(
      req({ planKey: 'p', instanceKey: 'I0', compilePlan: () => coarsePlan(['users']), notify }),
    )
    const lease = r.token.redeem() // wired subscriber
    registry.retirePlan('p') // retire owns the single teardown; fires one coarse invalidation
    expect(r.graph.state()).toBe('retired')
    expect(registry.inspect().graphs).toBe(0)
    expect(registry.inspect().factories).toBe(0) // plan dropped exactly once
    lease.release() // the lease was open at retire; its late release MUST be inert (no double teardown)
    expect(registry.inspect().graphs).toBe(0) // still gone — no resurrection, no negative accounting
    expect(registry.inspect().factories).toBe(0)
    expect(notify).toHaveBeenCalledTimes(1) // retire fired one coarse invalidation; the late release did not re-notify
  })
})

// ── §6 — db.live registry reopen re-certification (seams 1+2) ────────
// Each case discriminates a seam: reverting subscribe-mint→redeem (seam 1) breaks the inert-token
// assertions; reverting the seqAtRead/dead-entry fence (seam 2) breaks the catch-up assertions.

describe('§6.1/6.2 — un-redeemed token is inert; the redeem fence replays exactly once', () => {
  it('a change routed during the read window does not notify the inert token, then redeem fires it once', async () => {
    const registry = registryOf()
    const notify = vi.fn()
    const r = await registry.acquire(
      req({ instanceKey: 'inst-fence', compilePlan: () => coarsePlan(['users']), notify }),
    )
    // Read window: minted but NOT redeemed → the token has joined no sink (seam 1).
    registry.router.ingest({ changes: [{ table: 'users', kind: 'insert', new: { id: 1 } }] })
    expect(notify).toHaveBeenCalledTimes(0) // seam 1: nothing fires into the not-yet-wired channel
    r.token.redeem()
    expect(notify).toHaveBeenCalledTimes(1) // seam 2: the fence replays the missed change exactly once
  })
})

describe('§6.3 — a clean redeem fires no notify', () => {
  it('redeem with no change since the σ-read does not spuriously fire', async () => {
    const registry = registryOf()
    const notify = vi.fn()
    const r = await registry.acquire(req({ compilePlan: () => coarsePlan(['users']), notify }))
    r.token.redeem() // seqAtRead === invalidationSeq() → fence dormant
    expect(notify).toHaveBeenCalledTimes(0)
  })
})

describe('§6.4 — drift during the read window reaches the token at redeem', () => {
  it('retirePlan before redeem: the dead-entry redeem fires one catch-up notify and takes no lease/subscription', async () => {
    const registry = registryOf()
    const notify = vi.fn()
    const r = await registry.acquire(
      req({ planKey: 'q@e1', instanceKey: 'I1', compilePlan: () => coarsePlan(['users']), notify }),
    )
    registry.retirePlan('q@e1') // drift during the read window: the entry is finalized (dead), its sink dropped
    expect(notify).toHaveBeenCalledTimes(0) // the un-redeemed token was never subscribed → retire fired nothing to it
    const lease = r.token.redeem() // dead-entry branch: one catch-up notify, no lease/subscription
    expect(notify).toHaveBeenCalledTimes(1)
    expect(registry.inspect().graphs).toBe(0) // no resurrection — the dead identity stays torn down
    lease.release() // the no-op lease is inert
    expect(registry.inspect().graphs).toBe(0)
  })

  it('retirePlan before release: releasing the un-redeemed token after drift is inert', async () => {
    const registry = registryOf()
    const notify = vi.fn()
    const r = await registry.acquire(
      req({ planKey: 'q@e2', instanceKey: 'I2', compilePlan: () => coarsePlan(['users']), notify }),
    )
    registry.retirePlan('q@e2')
    r.token.release() // un-redeemed, entry already dead → inert, no throw, no re-notify
    expect(registry.inspect().graphs).toBe(0)
    expect(notify).toHaveBeenCalledTimes(0) // never subscribed → retire and release both silent
  })
})

describe('§6.5 — a never-redeemed token disposes net-zero', () => {
  it('the eagerly-hydrated graph is created then fully disposed on release; zero fires', async () => {
    const registry = registryOf()
    const notify = vi.fn()
    const r = await registry.acquire(
      req({ compilePlan: () => statefulPlan(['users'], [seed('users', 'users')]), notify }),
    )
    expect(registry.inspect().graphs).toBe(1) // eager hydrate (synchronous seed) allocated the graph
    r.token.release() // never serialized → release the un-redeemed token
    expect(r.graph.state()).toBe('destroyed')
    expect(registry.inspect().graphs).toBe(0) // NET-ZERO: created-then-disposed, back to baseline
    expect(notify).toHaveBeenCalledTimes(0) // never wired → zero fires
  })
})

describe('§6.6 — redeem transfers with no transient zero; double-redeem throws', () => {
  it('redeem keeps the graph alive across the token→lease transfer and rejects a second redeem', async () => {
    const registry = registryOf()
    const r = await registry.acquire(req({ compilePlan: () => coarsePlan(['users']) }))
    const lease = r.token.redeem()
    expect(r.graph.state()).not.toBe('destroyed') // never dipped to zero during the transfer
    expect(() => r.token.redeem()).toThrow() // double redeem rejected
    lease.release()
    expect(r.graph.state()).toBe('destroyed')
  })
})

describe('§6.4b — a dead redeem does not resurrect the sink for a re-acquired identity', () => {
  it('after a dead redeem, a fresh acquire of the SAME instanceKey notifies ONLY the fresh subscriber', async () => {
    const registry = registryOf()
    const oldNotify = vi.fn()
    const first = await registry.acquire(
      req({ planKey: 'q@e1', instanceKey: 'I', compilePlan: () => coarsePlan(['users']), notify: oldNotify }),
    )
    registry.retirePlan('q@e1') // drift: the entry is finalized (dead) and its sink dropped
    first.token.redeem() // dead branch: one catch-up notify, MUST NOT subscribe (no sink resurrection)
    expect(oldNotify).toHaveBeenCalledTimes(1)

    // Re-acquire the SAME instanceKey under a new epoch → a fresh instance + fresh subscriber.
    const freshNotify = vi.fn()
    const second = await registry.acquire(
      req({ planKey: 'q@e2', instanceKey: 'I', compilePlan: () => coarsePlan(['users']), notify: freshNotify }),
    )
    second.token.redeem() // a wired subscriber for the fresh instance
    registry.router.ingest({ changes: [{ table: 'users', kind: 'insert', new: { id: 1 } }] })
    expect(freshNotify).toHaveBeenCalledTimes(1) // the fresh subscriber hears the change
    expect(oldNotify).toHaveBeenCalledTimes(1) // STILL 1 — the dead-redeemed notify was NOT resurrected into the sink
  })
})

describe('§6.6b — leases refcount: a non-last close does not dispose while another owner holds', () => {
  it('two redeemed owners of one instance — closing EITHER first keeps the graph live + the survivor notifying; last close disposes', async () => {
    for (const closeFirst of ['A', 'B'] as const) {
      const registry = registryOf()
      const nA = vi.fn()
      const nB = vi.fn()
      const a = await registry.acquire(
        req({ instanceKey: 'shared', compilePlan: () => coarsePlan(['users']), notify: nA }),
      )
      const b = await registry.acquire(
        req({ instanceKey: 'shared', compilePlan: () => coarsePlan(['users']), notify: nB }),
      )
      expect(a.graph).toBe(b.graph) // one shared graph, two owners
      const leaseA = a.token.redeem()
      const leaseB = b.token.redeem()
      const [firstClose, survivorNotify, closedNotify, lastClose] =
        closeFirst === 'A' ? ([leaseA, nB, nA, leaseB] as const) : ([leaseB, nA, nB, leaseA] as const)

      firstClose.release() // a NON-last close
      expect(registry.inspect().graphs).toBe(1) // graph SURVIVES — the other owner still holds a lease
      expect(a.graph.state()).not.toBe('destroyed')
      registry.router.ingest({ changes: [{ table: 'users', kind: 'insert', new: { id: 1 } }] })
      expect(survivorNotify).toHaveBeenCalledTimes(1) // the survivor still notifies
      expect(closedNotify).toHaveBeenCalledTimes(0) // the CLOSED owner unsubscribed on release → silent (kills remove-unsubscribe)

      lastClose.release() // the last close
      expect(registry.inspect().graphs).toBe(0) // disposed only at zero refs
    }
  })
})

// ── §6 fence × router-owned demotions (fault/coarsen must advance seq) ──

describe('§6.2b — a coarse event during the read window is caught by the redeem fence', () => {
  it('a coarse marker routed after the σ-read (before redeem) fires notify exactly once at redeem', async () => {
    const registry = registryOf()
    const notify = vi.fn()
    const r = await registry.acquire(
      req({ instanceKey: 'inst-coarse', compilePlan: () => coarsePlan(['users']), notify }),
    )
    registry.router.ingest({ changes: [{ table: 'users', kind: 'coarse' }] }) // read window: coarsen() demotes; the inert token isn't subscribed
    expect(notify).toHaveBeenCalledTimes(0) // seam 1: nothing fires into the not-yet-wired channel
    r.token.redeem()
    expect(notify).toHaveBeenCalledTimes(1) // fence caught the coarsen — coarsen()'s fire('coarse') advanced seq past seqAtRead
  })
})

describe('§6.2c — an apply-fault during the read window is caught by the redeem fence', () => {
  it('a fault (what the router does on an apply-throw) after the σ-read fires notify exactly once at redeem', async () => {
    const registry = registryOf()
    const notify = vi.fn()
    const r = await registry.acquire(
      req({ instanceKey: 'inst-fault', compilePlan: () => coarsePlan(['users']), notify }),
    )
    r.graph.fault() // the router faults a throwing graph during the read window; the inert token isn't subscribed
    expect(notify).toHaveBeenCalledTimes(0) // seam 1: inert token
    r.token.redeem()
    expect(notify).toHaveBeenCalledTimes(1) // fence caught the fault — fault()'s fire('coarse') advanced seq past seqAtRead
  })
})
