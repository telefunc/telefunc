import { describe, expect, it, vi } from 'vitest'
import { derived, LiveCell } from './live.js'
import { liveReplacer } from './wireServer.js'
import type { ServerReplacerContext } from 'telefunc'

// SERIALIZE-TIME ACTIVATION, exercised through THIS package's own replacer.
//
// It used to drive telefunc's `createStreamingReplacer` and the real json-serializer. That reached into
// core internals, which this package deliberately no longer does — so the harness now calls
// `liveReplacer.replace` directly for each Live in the value. That is the same seam the serializer calls;
// what is no longer covered here is the serializer's own walk (does it FIND a nested Live?), which is
// core's behaviour rather than this package's, and which the multi-instance browser e2e proves end to end.

// A macrotask flush drains the whole microtask chain (a dep's coalesced flush → derived.invalidate →
// the derived's own coalesced flush → channel send).
const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

type FakeChannel = { id: string; sends: unknown[]; fireClose: () => void }
function makeFakeChannel(id: string): FakeChannel & Record<string, unknown> {
  const sends: unknown[] = []
  const closeCbs: Array<() => void> = []
  return {
    id,
    sends,
    fireClose: () => closeCbs.forEach((cb) => cb()),
    send: (event: unknown) => {
      sends.push(event)
      return Promise.resolve()
    },
    onClose: (cb: () => void) => closeCbs.push(cb),
    close: () => Promise.resolve(),
    abort: () => {},
  }
}

function createServerHarness() {
  const created: Array<FakeChannel & Record<string, unknown>> = []
  let n = 0
  const context = {
    createChannel: () => {
      const channel = makeFakeChannel(`ch-${n++}`)
      created.push(channel)
      return channel as never
    },
    registerChannel: () => {},
    validators: new Map(),
  } as unknown as ServerReplacerContext
  /** Walk the value the way the serializer does and hand every Live to our replacer. */
  const serialize = (value: unknown): void => {
    const visit = (v: unknown): void => {
      if (v === null || typeof v !== 'object') return
      if (liveReplacer.detect(v)) {
        liveReplacer.replace(v, context)
        return
      }
      for (const child of Object.values(v as Record<string, unknown>)) visit(child)
    }
    visit(value)
  }
  return { serialize, created }
}

// `Live.derived` returns the public `Live<R>` (just `{ readonly data }`); the object behind it IS the
// derived cell, which is what the activation-state introspection reads.
const asCell = (live: unknown) => live as LiveCell<unknown>

describe('derived — deferred cascade activation + cell-local leasing', () => {
  it('inert pending descriptors — a never-serialized derived subscribes NOTHING', () => {
    const subscribe = vi.fn(() => () => {})
    const a = new LiveCell('a')
    a.attachSource({ subscribe })
    const b = new LiveCell('b')
    derived(() => `${a.data}|${b.data}`) // reads a.data + b.data → tracks {a,b}
    // Never serialized → the tracked deps stay INERT: nothing subscribes their sources. This is the
    // leak the deferred design exists to prevent — if `derived` activated at CALL time, it fires.
    expect(subscribe).not.toHaveBeenCalled()
  })

  it('multi-dep invalidate forwarding once serialized', async () => {
    const a = new LiveCell(1)
    const b = new LiveCell(2)
    const server = createServerHarness()
    server.serialize(derived(() => a.data + b.data)) // tracks {a,b}
    const derivedChannel = server.created[0]!
    a.invalidate()
    await flush()
    expect(derivedChannel.sends).toEqual([undefined])
    b.invalidate()
    await flush()
    expect(derivedChannel.sends).toEqual([undefined, undefined])
  })

  it('zero-dep derived is inert (no dep subscriptions) but serializes as a normal live cell', async () => {
    const server = createServerHarness()
    const zeroDep = derived(() => 42) // reads no handle
    server.serialize(zeroDep)
    expect(server.created).toHaveLength(1)
    // It behaves as an ordinary cell on its own channel: driving it emits, with no cascade to speak of.
    asCell(zeroDep).invalidate()
    await flush()
    expect(server.created[0]!.sends).toEqual([undefined])
  })

  it('shared-dep-activated-once — a dep both returned and read by a derived activates its source ONCE', () => {
    const teardown = vi.fn()
    const subscribe = vi.fn(() => teardown)
    const a = new LiveCell('a')
    a.attachSource({ subscribe })
    const server = createServerHarness()
    server.serialize({ a, summary: derived(() => `sum:${a.data}`) })
    expect(server.created).toHaveLength(2) // a's channel + the derived's channel
    expect(subscribe).toHaveBeenCalledTimes(1) // idempotent activate: the shared source subscribes once
    // (That `a` holds one lease PER owning channel is proven behaviorally by the close-order test
    // below — the survivor stays live and the source tears down exactly once.)
  })

  it('both close orders — the survivor stays live; the shared source tears down exactly once', async () => {
    // close-dep-channel-first → the derived still fires
    {
      const teardown = vi.fn()
      const a = new LiveCell('a')
      a.attachSource({ subscribe: () => teardown })
      const server = createServerHarness()
      server.serialize({ a, summary: derived(() => a.data) })
      const [aChannel, derivedChannel] = server.created
      aChannel!.fireClose() // close the dep's OWN channel first
      expect(teardown).not.toHaveBeenCalled() // a still holds the derived's lease
      a.invalidate()
      await flush()
      expect(derivedChannel!.sends).toEqual([undefined]) // derived still fires
      derivedChannel!.fireClose()
      expect(teardown).toHaveBeenCalledTimes(1) // last owner → torn down exactly once
    }
    // close-derived-channel-first → the dep still fires
    {
      const teardown = vi.fn()
      const a = new LiveCell('a')
      a.attachSource({ subscribe: () => teardown })
      const server = createServerHarness()
      server.serialize({ a, summary: derived(() => a.data) })
      const [aChannel, derivedChannel] = server.created
      derivedChannel!.fireClose() // close the derived's channel first
      expect(teardown).not.toHaveBeenCalled() // a still holds its own channel's lease
      a.invalidate()
      await flush()
      expect(aChannel!.sends).toEqual([undefined]) // the dep still fires on its own channel
      aChannel!.fireClose()
      expect(teardown).toHaveBeenCalledTimes(1) // last owner → torn down exactly once
    }
  })
})
