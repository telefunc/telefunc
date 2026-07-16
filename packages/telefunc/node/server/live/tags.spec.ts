import '../context/async.js' // install AsyncLocalStorage mode so context survives macrotask awaits
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { restoreContext } from '../context/context.js'
import { getTagHub, _resetTagHubsForTesting, _setBarrierBudgetForTesting } from './tagHub.js'
import { liveTag, invalidateTag, stampRequestStartFence, publishQueuedTags } from './tags.js'
import { takeLiveSources } from './source.js'
import {
  getBroadcastAdapter,
  _resetBroadcastAdapterForTesting,
  DefaultBroadcastAdapter,
} from '../../../wire-protocol/server/broadcast.js'
import type { BroadcastTransport } from '../../../wire-protocol/server/broadcast.js'
import { parse } from '@brillout/json-serializer/parse'

let previousAdapter: ReturnType<typeof getBroadcastAdapter>

beforeEach(() => {
  previousAdapter = getBroadcastAdapter()
  _resetBroadcastAdapterForTesting(new DefaultBroadcastAdapter()) // fresh in-memory adapter (seq resets)
  _resetTagHubsForTesting()
})
afterEach(() => {
  _resetBroadcastAdapterForTesting(previousAdapter)
})

function inRequest<T>(fn: () => Promise<T>): Promise<T> {
  return restoreContext({}, fn)
}

function subscribeFirstSource(onInvalidate: () => void): void {
  const [source] = takeLiveSources()
  source!.subscribe(onInvalidate)
}

/** Publish calls carrying a tag batch (readiness-barrier frames are `{"barrier":...}`, no tags). */
function tagBatchCalls(spy: ReturnType<typeof vi.spyOn>): unknown[][] {
  return spy.mock.calls.filter((c) => typeof c[1] === 'string' && (c[1] as string).includes('"tags"'))
}

describe('tag fences (§3.E)', () => {
  it('T1.E1a fence — liveTag-AFTER-read window replays exactly once', () =>
    inRequest(async () => {
      await stampRequestStartFence()
      await getTagHub().publish(['t'])
      liveTag('t')
      const onInvalidate = vi.fn()
      subscribeFirstSource(onInvalidate)
      expect(onInvalidate).toHaveBeenCalledTimes(1)
    }))

  it('T1.E1b fence — liveTag-BEFORE-publish window replays exactly once', () =>
    inRequest(async () => {
      await stampRequestStartFence()
      liveTag('t')
      await getTagHub().publish(['t'])
      const onInvalidate = vi.fn()
      subscribeFirstSource(onInvalidate)
      expect(onInvalidate).toHaveBeenCalledTimes(1)
    }))

  it('T1.E2 negative — non-matching tag', () =>
    inRequest(async () => {
      await stampRequestStartFence()
      liveTag('t')
      await getTagHub().publish(['other'])
      const onInvalidate = vi.fn()
      subscribeFirstSource(onInvalidate)
      expect(onInvalidate).toHaveBeenCalledTimes(0)
    }))

  it('T1.E3 negative — tag published before the fence', () =>
    inRequest(async () => {
      await getTagHub().ready()
      await getTagHub().publish(['t']) // observed BEFORE the fence stamp
      await stampRequestStartFence()
      liveTag('t')
      const onInvalidate = vi.fn()
      subscribeFirstSource(onInvalidate)
      expect(onInvalidate).toHaveBeenCalledTimes(0)
    }))

  it('T1.E4/E15 journal overflow → unconditional replay', () =>
    inRequest(async () => {
      await stampRequestStartFence()
      for (let i = 0; i < 1030; i++) await getTagHub().publish(['t'])
      liveTag('t')
      const onInvalidate = vi.fn()
      subscribeFirstSource(onInvalidate)
      expect(onInvalidate).toHaveBeenCalledTimes(1)
    }))

  it('T1.E5 live tag after subscribe fires via the index; non-matching does not', () =>
    inRequest(async () => {
      await stampRequestStartFence()
      liveTag('t')
      const onInvalidate = vi.fn()
      subscribeFirstSource(onInvalidate)
      expect(onInvalidate).toHaveBeenCalledTimes(0)
      await getTagHub().publish(['t'])
      expect(onInvalidate).toHaveBeenCalledTimes(1)
      await getTagHub().publish(['other'])
      expect(onInvalidate).toHaveBeenCalledTimes(1)
    }))

  it('T1.E11 attach ordering — a post-subscribe publish makes the source emit exactly once', () =>
    inRequest(async () => {
      await stampRequestStartFence()
      liveTag('t')
      const onInvalidate = vi.fn()
      subscribeFirstSource(onInvalidate) // subscribed, no peer attached yet
      expect(onInvalidate).toHaveBeenCalledTimes(0)
      await getTagHub().publish(['t']) // would be buffered by the channel until attach (Sprint 2)
      expect(onInvalidate).toHaveBeenCalledTimes(1)
    }))

  it('T1.E10 the ready barrier is awaited once and reused only after a successful proof', () =>
    inRequest(async () => {
      const publishSpy = vi.spyOn(getBroadcastAdapter(), 'publish')
      await getTagHub().ready() // probes the barrier once (in-memory: one round-trip)
      const afterFirst = publishSpy.mock.calls.length
      expect(afterFirst).toBeGreaterThan(0)
      await getTagHub().ready() // resolved barrier is cached — no re-probe
      expect(publishSpy.mock.calls.length).toBe(afterFirst)
    }))
})

describe('tag settle + publish (§3.D / §3.E)', () => {
  it('T1.E6/D1 invalidateTag queues; settle publishes one deduped batch', () =>
    inRequest(async () => {
      await stampRequestStartFence()
      const publishSpy = vi.spyOn(getBroadcastAdapter(), 'publish')
      invalidateTag('t')
      invalidateTag('t')
      invalidateTag('u')
      expect(tagBatchCalls(publishSpy)).toHaveLength(0) // not during the body
      await publishQueuedTags()
      const batches = tagBatchCalls(publishSpy)
      expect(batches).toHaveLength(1) // exactly one batch
      const batch = parse(batches[0]![1] as string) as { tags: string[] }
      expect(new Set(batch.tags)).toEqual(new Set(['t', 'u']))
    }))

  it('T1.E9 invalidateTag outside a request asserts', async () => {
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(() => invalidateTag('t')).toThrow()
  })
})

describe('tag readiness barrier (§3.E T1.E10)', () => {
  // Transport whose listen() only starts delivering after `activateAfterMs`; earlier sends are
  // acknowledged but dropped. The barrier must wait until delivery is proven before the fence stamp.
  function delayedTransport(activateAfterMs: number): BroadcastTransport {
    let active = false
    let seq = 0
    const listeners = new Map<string, (p: string, i: { seq: number; timestamp: number }) => void>()
    setTimeout(() => {
      active = true
    }, activateAfterMs)
    return {
      send: (key, payload) => {
        seq++
        const info = { seq, timestamp: 0 }
        if (active) listeners.get(key)?.(payload, info)
        return Promise.resolve(info)
      },
      listen: (key, onMessage) => {
        listeners.set(key, onMessage)
        return () => listeners.delete(key)
      },
      sendBinary: () => Promise.resolve({ seq: 0, timestamp: 0 }),
      listenBinary: () => () => {},
    }
  }

  beforeEach(() => {
    _resetBroadcastAdapterForTesting(new DefaultBroadcastAdapter(delayedTransport(10)))
    _resetTagHubsForTesting()
  })

  it('T1.E10 ready() blocks until the subscription delivers; a publish after the read replays', () =>
    inRequest(async () => {
      await stampRequestStartFence() // blocks until the barrier is observed (subscription active)
      await getTagHub().publish(['t']) // guaranteed delivered now
      liveTag('t')
      const onInvalidate = vi.fn()
      subscribeFirstSource(onInvalidate)
      expect(onInvalidate).toHaveBeenCalledTimes(1)
    }))
})

describe('readiness fails closed when unproven (§3.E T1.E10)', () => {
  // Transport that acknowledges every send but NEVER delivers — the exact async-SUBSCRIBE hole. The
  // barrier must never resolve unproven; it fails closed instead of proceeding best-effort.
  function neverDeliveringTransport(): BroadcastTransport {
    let seq = 0
    return {
      send: () => Promise.resolve({ seq: ++seq, timestamp: 0 }), // acknowledged, then dropped
      listen: () => () => {},
      sendBinary: () => Promise.resolve({ seq: 0, timestamp: 0 }),
      listenBinary: () => () => {},
    }
  }

  beforeEach(() => {
    _resetBroadcastAdapterForTesting(new DefaultBroadcastAdapter(neverDeliveringTransport()))
    _resetTagHubsForTesting()
    _setBarrierBudgetForTesting(3) // fail fast instead of the production budget
  })
  afterEach(() => _setBarrierBudgetForTesting(50))

  it('T1.E10 ready() rejects (fail-closed) when the subscription never confirms', () =>
    inRequest(async () => {
      await expect(stampRequestStartFence()).rejects.toThrow() // request errors, no silent false negative
    }))

  it('T1.E10 a failed barrier is not cached — a later ready() re-probes', () =>
    inRequest(async () => {
      const publishSpy = vi.spyOn(getBroadcastAdapter(), 'publish')
      await expect(getTagHub().ready()).rejects.toThrow()
      const afterFirst = publishSpy.mock.calls.length
      await expect(getTagHub().ready()).rejects.toThrow()
      expect(publishSpy.mock.calls.length).toBeGreaterThan(afterFirst) // re-probed, not a cached rejection
    }))
})

describe('readiness barrier state is bounded across cycles (rubric §3 / T1.E10)', () => {
  // Transport that buffers every send and only delivers on an explicit flush() — deterministically
  // simulating barrier frames that arrive AFTER their (already-failed) readiness attempt settled.
  let flush: () => void

  beforeEach(() => {
    let seq = 0
    const listeners = new Map<string, (payload: string, info: { seq: number; timestamp: number }) => void>()
    const buffered: Array<{ key: string; payload: string; info: { seq: number; timestamp: number } }> = []
    const transport: BroadcastTransport = {
      send: (key, payload) => {
        const info = { seq: ++seq, timestamp: 0 }
        buffered.push({ key, payload, info })
        return Promise.resolve(info)
      },
      listen: (key, onMessage) => {
        listeners.set(key, onMessage)
        return () => listeners.delete(key)
      },
      sendBinary: () => Promise.resolve({ seq: 0, timestamp: 0 }),
      listenBinary: () => () => {},
    }
    flush = () => {
      for (const frame of buffered) listeners.get(frame.key)?.(frame.payload, frame.info)
      buffered.length = 0
    }
    _resetBroadcastAdapterForTesting(new DefaultBroadcastAdapter(transport))
    _resetTagHubsForTesting()
    _setBarrierBudgetForTesting(1) // each cycle fails after one un-delivered attempt
  })
  afterEach(() => _setBarrierBudgetForTesting(50))

  it('T1.E10 five fail/recover cycles retain zero barrier tokens', async () => {
    const hub = getTagHub()
    for (let i = 0; i < 5; i++) {
      await expect(hub.ready()).rejects.toThrow() // fails closed; the barrier frame is only buffered
    }
    flush() // deliver all 5 buffered barrier frames — every one from an already-settled cycle
    // A growing Set would now hold 5 tokens; the current-token field holds none.
    expect(hub._retainedBarrierTokenCountForTesting()).toBe(0)
  })
})

describe('tag publish failure (§3.D T1.D2 / T1.J4)', () => {
  it('T1.D2/J4 publish failure is logged + counted + fired locally, result unmasked', () =>
    inRequest(async () => {
      await stampRequestStartFence() // barrier established over the working in-memory adapter
      const localListener = vi.fn()
      getTagHub().registerTag('t', localListener) // an external local subscriber
      // Only the settle publish fails (transport down at write time), not readiness:
      vi.spyOn(getBroadcastAdapter(), 'publish').mockRejectedValue(new Error('transport down'))
      const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
      invalidateTag('t')
      await expect(publishQueuedTags()).resolves.toBeUndefined() // never throws
      expect(localListener).toHaveBeenCalledTimes(1) // fired locally despite the failure
      expect(errorSpy).toHaveBeenCalled()
      errorSpy.mockRestore()
    }))
})
