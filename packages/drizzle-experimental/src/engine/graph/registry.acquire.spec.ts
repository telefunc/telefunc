// ACQUIRE AND OWNERSHIP: who gets which graph, and when it is torn down.
//
// Canonical acquire dedup (A1), concurrent-share + failure recovery (A2), activate-before-read spy order
// (A3), born-state by plan class (A4), refcount = leases + unredeemed tokens with atomic redeem-transfer and
// immediate destroy at zero (A5/A7), state-row bound + no-PK born-coarse (A6), and per-instance FRESH compile
// (no cross-instance plan cache).
//
// The failure vocabulary here is ownership: two callers handed different graphs for the same query, a graph
// destroyed while someone still holds it, a graph kept alive after everyone let go, or a poisoned entry that
// makes every later acquire fail. WHO HEARS about a change is the other file's subject.

import { describe, expect, it, vi } from 'vitest'
import type { GraphPlan } from '../compile/compile.js'
import type { Row } from '../compile/rowSpace.js'
import {
  coarsePlan,
  deferred,
  fakeExecutor,
  registryOf,
  req,
  seed,
  statefulPlan,
  statelessPlan,
} from './registry.testKit.js'

// ── dedup + concurrent creation ───────────────────────────

describe('canonical acquire dedup', () => {
  it('two acquires of the same identity return one shared graph, compiling once', async () => {
    const registry = registryOf()
    const compile = vi.fn(() => coarsePlan(['users']))
    const a = await registry.acquire(req({ compilePlan: compile }))
    const b = await registry.acquire(req({ compilePlan: compile }))
    expect(a.graph).toBe(b.graph) // deduped to ONE graph (the identical-instance sharing invariant)
    expect(compile).toHaveBeenCalledTimes(1) // the shared graph is compiled once
  })
})

describe('concurrent acquire shares one creation + failure recovery', () => {
  it('two concurrent acquires share one in-flight creation and receive the same graph', async () => {
    const registry = registryOf()
    const plan = deferred<GraphPlan>()
    const compile = vi.fn(() => plan.promise)
    const p1 = registry.acquire(req({ compilePlan: compile }))
    const p2 = registry.acquire(req({ compilePlan: compile }))
    plan.resolve(coarsePlan(['users']))
    const [a, b] = await Promise.all([p1, p2])
    expect(compile).toHaveBeenCalledTimes(1) // one shared in-flight creation
    expect(a.graph).toBe(b.graph)
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
    // The failed entry was cleared (no poison): the retry recompiles a fresh graph and succeeds.
    const retry = await registry.acquire(req({ compilePlan: compile }))
    expect(retry.graph.state()).toBe('coarse')
    expect(compile).toHaveBeenCalledTimes(2) // one shared failed creation + one retry
  })
})

// ── A3 — activate before read ───────────────────────────────────────

describe('inputs registered before the acquiring read', () => {
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

describe('born-state by plan class', () => {
  it('a stateless plan is born live', async () => {
    const r = await registryOf().acquire(req({ compilePlan: () => statelessPlan(['users']) }))
    expect(r.graph.state()).toBe('live')
  })
  it('a stateful plan is born LIVE after the synchronous seed (acquire blocks on it)', async () => {
    const r = await registryOf().acquire(req({ compilePlan: () => statefulPlan(['users'], [seed('users', 'users')]) }))
    expect(r.graph.state()).toBe('live')
  })
})

// ── per-instance fresh compile (no cross-instance plan cache) ───────

describe('registry — each distinct instance compiles FRESH (no cross-instance plan cache)', () => {
  it('two distinct instanceKeys get independent graphs, each compiled on its own', async () => {
    const registry = registryOf()
    const compile = vi.fn(() => coarsePlan(['users']))
    const a = await registry.acquire(req({ instanceKey: 'I1', compilePlan: compile }))
    const b = await registry.acquire(req({ instanceKey: 'I2', compilePlan: compile }))
    expect(a.graph).not.toBe(b.graph) // distinct identities → independent graphs + state
    expect(compile).toHaveBeenCalledTimes(2) // no shared plan cache — each new identity compiles itself
  })
})

// ── A5 / A7 — refcount, tokens, leases ──────────────────────────────

describe('refcount = leases + unredeemed tokens; immediate destroy on zero', () => {
  it('releasing the sole token (no redeem) destroys immediately', async () => {
    const registry = registryOf()
    const r = await registry.acquire(req({ compilePlan: () => coarsePlan(['users']) }))
    r.token.release()
    expect(r.graph.state()).toBe('destroyed') // dispose-at-zero: the graph is torn down
  })

  it('token→lease redeem is an atomic transfer with no transient zero (no destroy-then-recreate)', async () => {
    const registry = registryOf()
    const r = await registry.acquire(req({ compilePlan: () => coarsePlan(['users']) }))
    const lease = r.token.redeem()
    expect(r.graph.state()).not.toBe('destroyed') // never dipped to zero during the transfer
    lease.release()
    expect(r.graph.state()).toBe('destroyed')
  })
})

describe('ReadToken redeem-transfer + disposal', () => {
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
    expect(r.graph.state()).toBe('destroyed')
  })

  it('read-error disposal releases an unredeemed token exactly once (refcount stays correct)', async () => {
    const registry = registryOf()
    const r = await registry.acquire(req({ compilePlan: () => coarsePlan(['users']) }))
    r.token.release() // the initial read failed → dispose the unredeemed token
    expect(r.graph.state()).toBe('destroyed')
    r.token.release() // idempotent second dispose
    expect(r.graph.state()).toBe('destroyed')
  })
})

// ── A6 — state-row bound + no-PK born coarse ────────────────────────

describe('state-row bound → demote; no-PK input born coarse', () => {
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

describe('a never-redeemed token disposes net-zero', () => {
  it('the eagerly-hydrated graph is created then fully disposed on release; zero fires', async () => {
    const registry = registryOf()
    const notify = vi.fn()
    const r = await registry.acquire(
      req({ compilePlan: () => statefulPlan(['users'], [seed('users', 'users')]), notify }),
    )
    expect(r.graph.state()).toBe('live') // eager hydrate (synchronous seed) allocated + seeded the graph
    r.token.release() // never serialized → release the un-redeemed token
    expect(r.graph.state()).toBe('destroyed') // NET-ZERO: created-then-disposed
    expect(notify).toHaveBeenCalledTimes(0) // never wired → zero fires
  })
})

describe('redeem transfers with no transient zero; double-redeem throws', () => {
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

describe('leases refcount: a non-last close does not dispose while another owner holds', () => {
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
      expect(a.graph.state()).not.toBe('destroyed') // graph SURVIVES — the other owner still holds a lease
      registry.router.ingest({ changes: [{ table: 'users', kind: 'insert', new: { id: 1 } }] })
      expect(survivorNotify).toHaveBeenCalledTimes(1) // the survivor still notifies
      expect(closedNotify).toHaveBeenCalledTimes(0) // the CLOSED owner unsubscribed on release → silent

      lastClose.release() // the last close
      expect(a.graph.state()).toBe('destroyed') // disposed only at zero refs
    }
  })
})
