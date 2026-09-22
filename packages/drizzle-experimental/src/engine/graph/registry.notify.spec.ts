// WHO HEARS ABOUT A CHANGE, and exactly once.
//
// The db.live registry: multi-subscriber notify ownership, the precision RELAY (a precise graph notifies only
// on an affected change), subscribe-at-redeem (un-redeemed tokens are inert), the seqAtRead activation fence,
// the router-owned coarse/fault demotions that fence must catch, and reseed at the registry boundary.
//
// Each case discriminates a seam: reverting subscribe-mint→redeem (seam 1) breaks the inert-token assertions;
// reverting the seqAtRead fence (seam 2) breaks the catch-up assertions.
//
// Everything is observed THROUGH THE INTERFACE — `graph.state()` and notify — never an inspect() getter. WHICH
// graph a caller gets, and when it is destroyed, is `registry.acquire.spec.ts`'s subject.

import { describe, expect, it, vi } from 'vitest'
import type { GraphPlan } from '../compile/compile.js'
import type { HydrationExecutor } from './hydrate.js'
import type { LiveGraph } from './liveGraph.js'
import { createRegistry } from './registry.js'
import { coarsePlan, fakeExecutor, registryOf, req, seed, statefulPlan } from './registry.testKit.js'

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

// ── db.live registry: subscribe-at-redeem + the seqAtRead fence ──────

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
