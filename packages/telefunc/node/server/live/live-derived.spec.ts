import { describe, expect, it, vi } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'
import { createStreamingReplacer } from '../../../wire-protocol/server/response/registry.js'
import type { ServerReplacerContext } from '../../../wire-protocol/types.js'
import { Live } from './live.js'
import { _activationStateForTesting } from './testing.js'

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
    sendStream: () => ({ metadata: { __index: 0 }, close() {}, abort() {} }),
    validators: new Map(),
  } as unknown as ServerReplacerContext
  const replacer = createStreamingReplacer(
    () => context,
    () => {},
    [],
  )
  const serialize = (value: unknown) => stringify(value, { forbidReactElements: true, replacer })
  return { serialize, created }
}

// `Live.derived` returns `.client` (a ClientLive), whose underlying object IS the derived Live.
const asLive = (clientLive: unknown) => clientLive as unknown as Live<unknown>

describe('Live.derived — deferred cascade activation + cell-local leasing (§3.B)', () => {
  it('T12.B1/B2 inert pending descriptors — a never-serialized derived subscribes NOTHING', () => {
    const a = new Live('a')
    const b = new Live('b')
    const derived = Live.derived(() => `${a.data}|${b.data}`) // reads a.data + b.data → tracks {a,b}
    // Nothing serialized → zero listeners added on the deps, and the derived holds zero leases.
    const empty = { lease: 0, invalidateListeners: 0, dataListeners: 0, sourceActive: false }
    expect(_activationStateForTesting(a)).toEqual(empty)
    expect(_activationStateForTesting(b)).toEqual(empty)
    expect(_activationStateForTesting(asLive(derived)).lease).toBe(0)
  })

  it('T12.B2 multi-dep invalidate forwarding once serialized', async () => {
    const a = new Live(1)
    const b = new Live(2)
    const server = createServerHarness()
    server.serialize(Live.derived(() => a.data + b.data)) // tracks {a,b}
    const derivedChannel = server.created[0]!
    a.invalidate()
    await flush()
    expect(derivedChannel.sends).toEqual([{ kind: 'invalidate' }])
    b.invalidate()
    await flush()
    expect(derivedChannel.sends).toEqual([{ kind: 'invalidate' }, { kind: 'invalidate' }])
  })

  it('T12.B2 zero-dep derived is inert (no dep subscriptions) but serializes as a normal live cell', () => {
    const server = createServerHarness()
    const derived = Live.derived(() => 42) // reads no handle
    server.serialize(derived)
    expect(server.created).toHaveLength(1)
    expect(_activationStateForTesting(asLive(derived)).lease).toBe(1) // its own channel's lease, no cascade
  })

  it('shared-dep-activated-once — a dep both returned and read by a derived activates its source ONCE', () => {
    const teardown = vi.fn()
    const subscribe = vi.fn(() => teardown)
    const a = new Live('a')
    a._attachSource({ subscribe })
    const server = createServerHarness()
    server.serialize({ a: a.client, summary: Live.derived(() => `sum:${a.data}`) })
    expect(server.created).toHaveLength(2) // a's channel + the derived's channel
    expect(subscribe).toHaveBeenCalledTimes(1) // idempotent _activate: the shared source subscribes once
    expect(_activationStateForTesting(a).lease).toBe(2) // one lease per owning channel
  })

  it('both close orders — the survivor stays live; the shared source tears down exactly once', async () => {
    // close-dep-channel-first → the derived still fires
    {
      const teardown = vi.fn()
      const a = new Live('a')
      a._attachSource({ subscribe: () => teardown })
      const server = createServerHarness()
      server.serialize({ a: a.client, summary: Live.derived(() => a.data) })
      const [aChannel, derivedChannel] = server.created
      aChannel!.fireClose() // close the dep's OWN channel first
      expect(teardown).not.toHaveBeenCalled() // a still holds the derived's lease
      a.invalidate()
      await flush()
      expect(derivedChannel!.sends).toEqual([{ kind: 'invalidate' }]) // derived still fires
      derivedChannel!.fireClose()
      expect(teardown).toHaveBeenCalledTimes(1) // last owner → torn down exactly once
    }
    // close-derived-channel-first → the dep still fires
    {
      const teardown = vi.fn()
      const a = new Live('a')
      a._attachSource({ subscribe: () => teardown })
      const server = createServerHarness()
      server.serialize({ a: a.client, summary: Live.derived(() => a.data) })
      const [aChannel, derivedChannel] = server.created
      derivedChannel!.fireClose() // close the derived's channel first
      expect(teardown).not.toHaveBeenCalled() // a still holds its own channel's lease
      a.invalidate()
      await flush()
      expect(aChannel!.sends).toEqual([{ kind: 'invalidate' }]) // the dep still fires on its own channel
      aChannel!.fireClose()
      expect(teardown).toHaveBeenCalledTimes(1) // last owner → torn down exactly once
    }
  })
})
