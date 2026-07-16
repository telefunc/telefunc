import { afterEach, describe, expect, it } from 'vitest'
import { getRawContext, provideTelefuncContext } from 'telefunc'
import { drainPostSerializeDisposers } from 'telefunc/__internal'
import { reactiveDrizzle, _installDbLiveRuntime } from './reactiveDrizzle.js'
import type { DbLiveCarrier } from './reactiveDrizzle.js'
import { assembleDbLiveRuntime } from './dbLiveRuntime.js'

// ─────────────────────────────────────────────────────────────────────────────
// Ticket 6 §U3 — the db.live runtime wiring gate (JOINT with EngineFix's readCapture engine). Proves the
// CARRIER lifecycle [GEN] under PRODUCTION SYNC mode: the carrier is captured before the body's first
// await, so a POST-await `db.live.select()` still tracks its read token on the captured carrier even after
// `getRawContext()` has nulled (the sync-mode / no-async_hooks reality), and the R1 finally-sweep releases
// a token that was never activated (net-zero). The engine's real token mechanics live in EngineFix's
// readCapture engine tests; here a fake engine isolates the sync-context concern with a controllable token.
// ─────────────────────────────────────────────────────────────────────────────

const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

type FakeToken = { release: () => void }
type FakeEntry = { token: FakeToken; redeemed: boolean }

/** A fake read-capture engine honoring EngineFix's contract: a chainable thenable whose terminal `await`
 *  mints `{ token, redeemed:false }` onto the request carrier. `minted`/`released` expose what happened so
 *  the sync-mode + sweep assertions can inspect it without reaching into the real engine's internals. */
function installFakeEngine() {
  const minted: FakeEntry[] = []
  const released: FakeToken[] = []
  const wrapLiveSelect = (_baseBuilder: unknown, carrier: DbLiveCarrier, _db: unknown): unknown => {
    const wrapper: Record<string, unknown> = {
      from: () => wrapper,
      where: () => wrapper,
      then: (onFulfilled?: (value: unknown) => unknown, onRejected?: (reason: unknown) => unknown) =>
        Promise.resolve()
          .then(() => {
            const token: FakeToken = {
              release: () => {
                released.push(token)
              },
            }
            const entry: FakeEntry = { token, redeemed: false }
            ;(carrier as unknown as { mintedTokens: FakeEntry[] }).mintedTokens.push(entry)
            minted.push(entry)
            return { __fakeClientLive: true }
          })
          .then(onFulfilled, onRejected),
    }
    return wrapper
  }
  // Real carrier lifecycle (acquireCarrier + the sweep registration) + real captureMutation; fake engine.
  _installDbLiveRuntime({ ...assembleDbLiveRuntime(), wrapLiveSelect })
  return { minted, released }
}

// Typed shape for the fake db so the `LiveOf<>` transform sees a real builder chain (`.from().where()`
// awaiting to a result) — mirrors the compile-check in reactiveDrizzle.spec.ts. Runtime is the fake below.
type Row = { id: number }
interface SelectBuilder extends PromiseLike<Row[]> {
  from(t: unknown): SelectBuilder
  where(c: unknown): SelectBuilder
}
interface MockDb {
  tag: string
  select(): SelectBuilder
}

// A plain (non-live) awaitable builder; the base db the accessor wraps. `db.select()` routes here
// untouched (only `.live.select` is wrapped), so it never touches the carrier.
const plainBuilder: Record<string, unknown> = {
  from: () => plainBuilder,
  where: () => plainBuilder,
  then: (onFulfilled: (value: unknown) => unknown) => Promise.resolve([]).then(onFulfilled),
}
const baseDb = { tag: 'db', select: () => plainBuilder } as unknown as MockDb

// Flush the sync-mode null timer scheduled by provideTelefuncContext so it never leaks into the next test.
afterEach(async () => {
  await tick()
})

describe('db.live runtime — carrier lifecycle (Ticket 6 §U3)', () => {
  it('T6.S1 sync-mode: a POST-await db.live.select tracks via the CAPTURED carrier (getRawContext is null), and the finally-sweep disposes the un-activated token', async () => {
    const engine = installFakeEngine()

    // Seed PRODUCTION sync context — this schedules the first-macrotask null that caused the crisis.
    provideTelefuncContext({})
    const context = getRawContext()
    expect(context).not.toBe(null)

    // Acquire the reactive db at the TOP of the "telefunction" (before any await): captures the carrier.
    const db = reactiveDrizzle(baseDb)()

    // Cross a macrotask — sync context nulls (the exact regression condition).
    await tick()
    expect(getRawContext()).toBe(null) // we are past the danger point: ambient context is gone

    // POST-await live read: must still mint on the CAPTURED carrier, not ambient (null) context.
    const live = await db.live.select().from({}).where({})
    expect(live).toBeDefined()
    expect(engine.minted).toHaveLength(1)
    expect(engine.minted[0]!.redeemed).toBe(false) // never serialized/activated in this unit

    // The R1 finally-sweep (registered by acquireCarrier on the captured context) releases it — net-zero.
    drainPostSerializeDisposers(context!)
    expect(engine.released).toHaveLength(1)
    expect(engine.released[0]).toBe(engine.minted[0]!.token)
  })

  it('sweep skips an ACTIVATED token: a serialized (redeemed) handle keeps its channel-owned lease', async () => {
    const engine = installFakeEngine()
    provideTelefuncContext({})
    const context = getRawContext()

    const db = reactiveDrizzle(baseDb)()
    await db.live.select().from({})
    expect(engine.minted).toHaveLength(1)

    // Simulate serialize-time activation (the wire replacer redeems the token).
    engine.minted[0]!.redeemed = true

    drainPostSerializeDisposers(context!)
    expect(engine.released).toHaveLength(0) // activated → skipped; its lease is channel-owned
  })

  it('binding differential: `db.live.select()` mints a read token; plain `db.select()` does not', async () => {
    const engine = installFakeEngine()
    provideTelefuncContext({})

    const db = reactiveDrizzle(baseDb)()

    const plain = await db.select().from({}).where({}) // plain path — routed untouched
    expect(plain).toEqual([])
    expect(engine.minted).toHaveLength(0) // plain select never touches the carrier

    const live = await db.live.select().from({}).where({}) // live path — wrapped by the engine
    expect(live).toBeDefined()
    expect(engine.minted).toHaveLength(1) // exactly the live read minted a token
  })

  it('acquireCarrier throws outside a telefunction (no context to capture)', () => {
    installFakeEngine()
    // No provideTelefuncContext here (and afterEach flushed any prior test's null timer), so
    // getRawContext() is null at accessor-call time → the read guard fires.
    expect(() => reactiveDrizzle(baseDb)()).toThrow(/reactive db accessor must be called inside a telefunction/)
  })
})
