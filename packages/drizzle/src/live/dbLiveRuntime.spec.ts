import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { getRawContext, provideTelefuncContext } from 'telefunc'
import { drainPostSerializeDisposers } from 'telefunc/__internal'
import { reactiveDrizzle } from './reactiveDrizzle.js'
import type { DbLiveCarrier } from './reactiveDrizzle.js'

// ─────────────────────────────────────────────────────────────────────────────
// The db.live runtime wiring gate. Proves the
// CARRIER lifecycle under PRODUCTION SYNC mode: the carrier is captured before the body's first
// await, so a POST-await `db.select()…live()` still tracks its read token on the captured carrier even
// after `getRawContext()` has nulled (the sync-mode / no-async_hooks reality), and the finally-sweep
// releases a token that was never activated (net-zero). The engine's real token mechanics live in the
// readCapture engine tests; here a FAKE `wrapLiveSelect` (mocked) isolates the sync-context concern with a
// controllable token, while the REAL `acquireCarrier` + `disposeUnredeemedReads` run the lifecycle.
// ─────────────────────────────────────────────────────────────────────────────

const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

type FakeToken = { release: () => void }
type FakeEntry = { token: FakeToken; redeemed: boolean }

// Hoisted engine state the mocked `wrapLiveSelect` writes to — hoisted because the `vi.mock` factory is
// lifted above the imports, so it must close over hoisted state. Reset per test in `beforeEach`.
const engine = vi.hoisted(() => ({ minted: [] as FakeEntry[], released: [] as FakeToken[] }))

// Fake ONLY the engine's `wrapLiveSelect`; keep the REAL `disposeUnredeemedReads` (and everything else)
// so the real carrier lifecycle — `acquireCarrier`'s finally-sweep — runs end-to-end against
// controllable tokens. The wrapper's terminal `.live()` mints `{ token, redeemed:false }` onto the
// request carrier, honoring the read-capture contract; awaiting it (no `.live()`) forwards to plain rows
// and mints nothing.
vi.mock('./readCapture.js', async (importActual) => {
  const actual = await importActual<typeof import('./readCapture.js')>()
  const wrapLiveSelect = (_baseBuilder: unknown, carrier: DbLiveCarrier, _db: unknown): unknown => {
    const wrapper: Record<string, unknown> = {
      from: () => wrapper,
      where: () => wrapper,
      // plain terminal: `await db.select()…` resolves to rows and never touches the carrier.
      then: (onFulfilled?: (value: unknown) => unknown, onRejected?: (reason: unknown) => unknown) =>
        Promise.resolve([]).then(onFulfilled, onRejected),
      // live terminal: mint a token on the request carrier, then resolve to a fake Live.
      live: () =>
        Promise.resolve().then(() => {
          const token: FakeToken = {
            release: () => {
              engine.released.push(token)
            },
          }
          const entry: FakeEntry = { token, redeemed: false }
          ;(carrier as unknown as { mintedTokens: FakeEntry[] }).mintedTokens.push(entry)
          engine.minted.push(entry)
          return { __fakeLive: true }
        }),
    }
    return wrapper
  }
  return { ...actual, wrapLiveSelect }
})

// The base db the proxy wraps. Its `select()` return is irrelevant here — the fake `wrapLiveSelect`
// above builds its own chain — so a loose shape suffices for the input constraint.
type MockDb = { tag: string; select(): unknown }
const baseDb: MockDb = { tag: 'db', select: () => ({}) }

// The reactive-db shape the tests drive at runtime: every `select()` chain is live-capable (`.live()`)
// and is itself awaitable to plain rows. A hand-rolled cast — the real row-typed HKT contract is proven
// against real Drizzle in the dedicated contract spec.
interface LiveChain extends PromiseLike<unknown[]> {
  from(t: unknown): LiveChain
  where(c: unknown): LiveChain
  live(): Promise<unknown>
}
interface ReactiveMockDb {
  tag: string
  select(): LiveChain
}
// baseDb is a hand-rolled stub, not a real Drizzle db; cast past reactiveDrizzle's real-db type constraint
// (a TYPE surface covered by the contract spec) — the carrier lifecycle under test is runtime, not types.
const acquire = (): ReactiveMockDb => reactiveDrizzle(baseDb as never) as unknown as ReactiveMockDb

beforeEach(() => {
  engine.minted.length = 0
  engine.released.length = 0
})
// Flush the sync-mode null timer scheduled by provideTelefuncContext so it never leaks into the next test.
afterEach(async () => {
  await tick()
})

describe('db.live runtime — carrier lifecycle', () => {
  it('sync-mode: a POST-await db.select()…live() tracks via the CAPTURED carrier (getRawContext is null), and the finally-sweep disposes the un-activated token', async () => {
    // Seed PRODUCTION sync context — this schedules the first-macrotask null that caused the crisis.
    provideTelefuncContext({})
    const context = getRawContext()
    expect(context).not.toBe(null)

    // Acquire the reactive db at the TOP of the "telefunction" (before any await): captures the carrier.
    const db = acquire()

    // Cross a macrotask — sync context nulls (the exact regression condition).
    await tick()
    expect(getRawContext()).toBe(null) // we are past the danger point: ambient context is gone

    // POST-await live read: must still mint on the CAPTURED carrier, not ambient (null) context.
    const live = await db.select().from({}).where({}).live()
    expect(live).toBeDefined()
    expect(engine.minted).toHaveLength(1)
    expect(engine.minted[0]!.redeemed).toBe(false) // never serialized/activated in this unit

    // The finally-sweep (registered by acquireCarrier on the captured context) releases it — net-zero.
    drainPostSerializeDisposers(context!)
    expect(engine.released).toHaveLength(1)
    expect(engine.released[0]).toBe(engine.minted[0]!.token)
  })

  it('sweep skips an ACTIVATED token: a serialized (redeemed) handle keeps its channel-owned lease', async () => {
    provideTelefuncContext({})
    const context = getRawContext()

    const db = acquire()
    await db.select().from({}).live()
    expect(engine.minted).toHaveLength(1)

    // Simulate serialize-time activation (the wire replacer redeems the token).
    engine.minted[0]!.redeemed = true

    drainPostSerializeDisposers(context!)
    expect(engine.released).toHaveLength(0) // activated → skipped; its lease is channel-owned
  })

  it('binding differential: terminal `.live()` mints a read token; a plain `await` of the same chain does not', async () => {
    provideTelefuncContext({})

    const db = acquire()

    const plain = await db.select().from({}).where({}) // no `.live()` → plain rows, no capture
    expect(plain).toEqual([])
    expect(engine.minted).toHaveLength(0) // the un-terminated chain never touches the carrier

    const live = await db.select().from({}).where({}).live() // terminal `.live()` → wrapped read
    expect(live).toBeDefined()
    expect(engine.minted).toHaveLength(1) // exactly the live read minted a token
  })

  it('reactiveDrizzle throws outside a telefunction (no context to capture)', () => {
    // No provideTelefuncContext here (and afterEach flushed any prior test's null timer), so
    // getRawContext() is null at call time → the read guard fires.
    expect(() => reactiveDrizzle(baseDb as never)).toThrow(/reactiveDrizzle\(db\) must be called inside a telefunction/)
  })
})
