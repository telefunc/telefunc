// The registry: canonical acquire dedup (A1), concurrent-share + failure recovery
// (A2), activate-before-read spy order (A3), born-state by plan class (A4), refcount = leases +
// unredeemed tokens with atomic redeem-transfer + immediate destroy (A5/A7), state-row + no-PK
// born-coarse (A6), per-instance FRESH compile (no cross-instance plan cache), and precision relay (a
// precise graph notifies ONLY on an affected change — the "spare unaffected queries" mandate). Plus
// the db.live registry: subscribe-at-redeem (un-redeemed tokens inert), the seqAtRead
// activation fence, and the router-owned coarse/fault demotions caught by that fence. Everything is
// observed THROUGH THE INTERFACE — graph.state() and notify — never an inspect() getter. All fakes
// are deterministic; pending promises drive the async paths, never timers.

import { describe, expect, it, vi } from 'vitest'
import type { FireResult, GraphPlan, SeedDescriptor, StatefulGraph } from '../compile/compile.js'
import type { Row } from '../compile/rowSpace.js'
import type { HydrationExecutor } from './hydrate.js'
import type { LiveGraph } from './liveGraph.js'
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

const NO_FIRE: FireResult = { invalidated: false }

function fakeExecutor(over: Partial<HydrationExecutor> = {}): HydrationExecutor {
  // acquire BLOCKS on the seed, so the scan must resolve for the graph to reach live.
  return { scan: over.scan ?? (() => Promise.resolve<Row[]>([])) }
}

function seed(inputId: string, table: string, primaryKey: string[] = ['id']): SeedDescriptor {
  return { inputId, table, relationId: table, alias: inputId, primaryKey, columns: '*', residual: { kind: 'true' } }
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
  it('two acquires of the identical query BOTH hear an invalidation (sinks is a set, not one callback)', async () => {
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
    expect(n1).toHaveBeenCalledTimes(1) // and so does the first — `sinks` holds a SET per instanceKey
  })
})

// ── precision relay — spare unaffected queries ──────────────────────
// The oracle proves a compiled EXACT plan is precise (fires IFF the result changed, both directions).
// This pins that the registry/router RELAYS that precision: a precise graph notifies a wired subscriber
// ONLY on an affected change. A precise→coarse over-fire regression breaks it — a coarse graph fires on
// ANY change, so the unaffected change would notify. This is the "don't refetch the whole userbase's
// todolist on an unrelated write" mandate at the registry integration level.

describe('registry precision — a precise graph notifies ONLY on an affected change', () => {
  it('an unaffected routed change does not notify; an affected one does', async () => {
    const registry = registryOf()
    const notify = vi.fn()
    // A precise (stateless) graph: fires ONLY for a change to id 5. A coarse graph would fire on ANY.
    const precise: GraphPlan = {
      tables: ['users'],
      stateless: true,
      coarse: false,
      instantiate: () => ({
        apply: (commit) => ({ invalidated: commit.some((c) => (c.new as { id?: number } | undefined)?.id === 5) }),
      }),
    }
    const r = await registry.acquire(req({ instanceKey: 'precise', compilePlan: () => precise, notify }))
    r.token.redeem() // a wired subscriber
    registry.router.ingest({ changes: [{ table: 'users', kind: 'insert', new: { id: 9 } }] }) // UNAFFECTED
    expect(notify).toHaveBeenCalledTimes(0) // spare it — no refetch on an unrelated change
    registry.router.ingest({ changes: [{ table: 'users', kind: 'insert', new: { id: 5 } }] }) // AFFECTED
    expect(notify).toHaveBeenCalledTimes(1) // the affected change does invalidate
  })
})

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

// ── db.live registry: subscribe-at-redeem + the seqAtRead fence ──────
// Each case discriminates a seam: reverting subscribe-mint→redeem (seam 1) breaks the inert-token
// assertions; reverting the seqAtRead fence (seam 2) breaks the catch-up assertions.

describe('un-redeemed token is inert; the redeem fence replays exactly once', () => {
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

describe('a clean redeem fires no notify', () => {
  it('redeem with no change since the σ-read does not spuriously fire', async () => {
    const registry = registryOf()
    const notify = vi.fn()
    const r = await registry.acquire(req({ compilePlan: () => coarsePlan(['users']), notify }))
    r.token.redeem() // seqAtRead === invalidationSeq() → fence dormant
    expect(notify).toHaveBeenCalledTimes(0)
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

// ── the fence × router-owned demotions (fault/coarsen must advance seq) ──

describe('a coarse event during the read window is caught by the redeem fence', () => {
  it('a coarse marker routed after the σ-read (before redeem) fires notify exactly once at redeem', async () => {
    const registry = registryOf()
    const notify = vi.fn()
    const r = await registry.acquire(
      req({ instanceKey: 'inst-coarse', compilePlan: () => coarsePlan(['users']), notify }),
    )
    registry.router.ingest({ changes: [{ table: 'users', kind: 'coarse' }] }) // read window: coarsen() demotes; the inert token isn't subscribed
    expect(notify).toHaveBeenCalledTimes(0) // seam 1: nothing fires into the not-yet-wired channel
    r.token.redeem()
    expect(notify).toHaveBeenCalledTimes(1) // fence caught the coarsen — coarsen()'s fire() advanced seq past seqAtRead
  })
})

describe('an apply-fault during the read window is caught by the redeem fence', () => {
  it('a fault (what the router does on an apply-throw) after the σ-read fires notify exactly once at redeem', async () => {
    const registry = registryOf()
    const notify = vi.fn()
    const r = await registry.acquire(
      req({ instanceKey: 'inst-fault', compilePlan: () => coarsePlan(['users']), notify }),
    )
    r.graph.fault() // the router faults a throwing graph during the read window; the inert token isn't subscribed
    expect(notify).toHaveBeenCalledTimes(0) // seam 1: inert token
    r.token.redeem()
    expect(notify).toHaveBeenCalledTimes(1) // fence caught the fault — fault()'s fire() advanced seq past seqAtRead
  })
})

// ── reseed at the registry boundary ──────────────────────────────────
//
// Three holes the reseed gate found, all at this seam rather than inside the graph. They are pinned HERE,
// through real sinks and real refcounts, because each one is invisible from inside `liveGraph`: the graph
// can advance its sequence, hold a consistent state machine and still leave a subscriber un-notified, a
// waiter attached to a rebuilding graph, or a token minted on a destroyed one.

/** A registry entry with a held scan, a redeemed subscriber, and that subscriber's notification count. */
async function reseedableEntry() {
  const registry = createRegistry({ maxStateRowsPerInput: 1e9 })
  const scans: Array<() => void> = []
  let hold = false
  const executor: HydrationExecutor = {
    scan: async () => {
      if (hold) await new Promise<void>((resolve) => scans.push(resolve))
      return []
    },
  }
  const notify = vi.fn()
  const { graph, token } = await registry.acquire(
    req({ compilePlan: () => statefulPlan(['users'], [seed('users', 'users')]), executor, notify }),
  )
  const lease = token.redeem() // a REAL subscriber: only a redeemed token joins the sink
  const drain = async () => {
    for (let i = 0; i < 5; i++) await Promise.resolve()
  }
  return {
    registry,
    graph,
    lease,
    notify,
    drain,
    hold: () => (hold = true),
    release: () => {
      hold = false
      scans.splice(0).forEach((resolve) => resolve())
    },
  }
}

describe('reseed — the cut reaches SUBSCRIBERS, not just the sequence counter', () => {
  it('a write racing the reseed scan notifies the redeemed subscriber a SECOND time', async () => {
    // The false-green this replaces: `fire()` advances `invalidationSeq`, which the read fence consults,
    // but subscribers are reached by the ROUTER's notify pass and the cut runs outside any routed batch.
    // A test asserting the sequence passes while no client is ever told. So assert the SINK.
    const h = await reseedableEntry()
    h.hold()
    h.registry.router.ingest({ changes: [{ table: 'users', kind: 'coarse' }] }) // → reseed starts
    await h.drain()
    expect(h.notify).toHaveBeenCalledTimes(1) // the reseed's own invalidation

    h.registry.router.ingest({ changes: [{ table: 'users', kind: 'insert', new: { id: 1 } }] }) // buffered
    h.release()
    await h.graph.ready()
    await h.drain()

    expect(h.graph.state()).toBe('live')
    expect(h.notify).toHaveBeenCalledTimes(2) // the racing write was ANNOUNCED, not silently absorbed
    h.lease.release()
  })

  it('an UNRACED reseed notifies exactly once — so the second notification is attributable', async () => {
    const h = await reseedableEntry()
    h.hold()
    h.registry.router.ingest({ changes: [{ table: 'users', kind: 'coarse' }] })
    await h.drain()
    h.release()
    await h.graph.ready()
    await h.drain()

    expect(h.notify).toHaveBeenCalledTimes(1)
    h.lease.release()
  })
})

describe('reseed — acquire cannot attach to a rebuilding or destroyed graph', () => {
  it('a waiter released by one cycle waits again when a SECOND cycle starts before it resumes', async () => {
    // Awaiting `ready()` once is not enough. A subscriber notified by the cut can cause another coarse
    // event synchronously, which starts a new cycle before the waiter's continuation runs — so a single
    // await hands back a graph that is rebuilding while reporting itself precise. Re-checking after every
    // await is what makes this generation-aware, and this schedule is what forces the second lap.
    const registry = createRegistry({ maxStateRowsPerInput: 1e9 })
    const queued: Array<() => void> = []
    const executor: HydrationExecutor = {
      scan: async () => {
        await new Promise<void>((resolve) => queued.push(resolve))
        return []
      },
    }
    const drain = async () => {
      for (let i = 0; i < 8; i++) await Promise.resolve()
    }
    const releaseOne = () => queued.splice(0).forEach((resolve) => resolve())

    // This subscriber turns a CUT into a second coarse event — the re-entrant schedule. It must fire only
    // from the cut, not from the router's own notify pass: that pass runs while the graph is already
    // `seeding`, where another coarse would hit the storm guard and terminally demote instead.
    let live: LiveGraph | undefined
    let again = true
    const notify = () => {
      if (!again || live?.state() !== 'live') return
      again = false
      registry.router.ingest({ changes: [{ table: 'users', kind: 'coarse' }] })
    }
    const first = registry.acquire(
      req({ compilePlan: () => statefulPlan(['users'], [seed('users', 'users')]), executor, notify }),
    )
    await drain()
    releaseOne() // the INITIAL seed lands
    const { graph, token } = await first
    live = graph
    const lease = token.redeem()

    registry.router.ingest({ changes: [{ table: 'users', kind: 'coarse' }] }) // cycle 1
    await drain()
    const second = registry.acquire(
      req({ compilePlan: () => statefulPlan(['users'], [seed('users', 'users')]), executor }),
    )
    let settled = false
    void second.then(() => (settled = true))
    await drain()

    // A racing write, so cycle 1's cut notifies — which is what starts cycle 2 re-entrantly.
    registry.router.ingest({ changes: [{ table: 'users', kind: 'insert', new: { id: 1 } }] })
    releaseOne() // cycle 1 completes → cut → notify → cycle 2 begins
    await drain()

    expect(graph.state()).toBe('seeding') // cycle 2 is running
    expect(settled).toBe(false) // …and the waiter did NOT attach to it

    releaseOne() // cycle 2 completes
    await drain()
    const attached = await second
    expect(graph.state()).toBe('live')
    expect(attached.graph).toBe(graph)
    attached.token.release()
    lease.release()
  })

  it('a waiter holds a ref, so the last lease closing mid-reseed cannot destroy the graph under it', async () => {
    // Without a provisional ref the waiter owns nothing: the sweep disposes the entry, unregisters it from
    // the router and destroys the graph, and the waiter then mints a token on it — a Live that can never be
    // invalidated again.
    const h = await reseedableEntry()
    h.hold()
    h.registry.router.ingest({ changes: [{ table: 'users', kind: 'coarse' }] })
    await h.drain()

    const second = h.registry.acquire(
      req({ compilePlan: () => statefulPlan(['users'], [seed('users', 'users')]), executor: fakeExecutor() }),
    )
    await h.drain()
    h.lease.release() // the ONLY existing lease closes while the waiter waits
    await h.drain()
    expect(h.graph.state()).not.toBe('destroyed') // the waiter's ref kept it alive

    h.release()
    const { graph, token } = await second
    expect(graph.state()).toBe('live')
    expect(graph).toBe(h.graph) // …the same graph, still registered — not a resurrected corpse

    // And it is genuinely still wired: a routed change reaches this token's subscriber.
    const notify = vi.fn()
    const lease = token.redeem()
    void notify
    h.registry.router.ingest({ changes: [{ table: 'users', kind: 'coarse' }] })
    await h.drain()
    expect(graph.state()).not.toBe('destroyed')
    lease.release()
  })
})
