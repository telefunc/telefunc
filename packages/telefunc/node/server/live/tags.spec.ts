import '../context/async.js' // install AsyncLocalStorage mode so context survives macrotask awaits
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { restoreContext } from '../context/context.js'
import { getTagHub, _resetTagHubsForTesting, _setBarrierBudgetForTesting } from './tagHub.js'
import { stampRequestStartFence, captureTagFence, subscribeCapturedTag } from './tags.js'
import { LiveCell } from './live.js'
import {
  getBroadcastAdapter,
  _resetBroadcastAdapterForTesting,
  DefaultBroadcastAdapter,
} from '../../../wire-protocol/server/broadcast.js'
import type { BroadcastTransport } from '../../../wire-protocol/server/broadcast.js'
import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'

let previousAdapter: ReturnType<typeof getBroadcastAdapter>

beforeEach(() => {
  previousAdapter = getBroadcastAdapter()
  _resetBroadcastAdapterForTesting(new DefaultBroadcastAdapter()) // fresh in-memory adapter (seq resets)
  _resetTagHubsForTesting()
})
afterEach(() => {
  _resetBroadcastAdapterForTesting(previousAdapter)
})

/** Let a fire-and-forget publish settle (invalidate is `void`-returning by design). */
const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

function inRequest<T>(fn: () => Promise<T>): Promise<T> {
  return restoreContext({}, fn)
}

/** Capture a tag fence and subscribe it — the low-level catch-up path `LiveCell.onInvalidate` uses at
 *  serialization: register the index listener, then replay a publish that landed since the fence. */
function subscribeFenced(tag: string, onInvalidate: () => void): () => void {
  return subscribeCapturedTag(captureTagFence(tag), onInvalidate)
}

/** Publish calls carrying a tag batch (readiness-barrier frames are `{"barrier":...}`, no tags). */
function tagBatchCalls(spy: ReturnType<typeof vi.spyOn>): unknown[][] {
  return spy.mock.calls.filter((c) => typeof c[1] === 'string' && (c[1] as string).includes('"tags"'))
}

describe('tag fences (§3.E)', () => {
  it('T1.E1 fence — a publish that landed since the request-start fence replays exactly once on subscribe', () =>
    inRequest(async () => {
      await stampRequestStartFence()
      await getTagHub().publish(['t']) // lands after the fence, before this request subscribes
      const onInvalidate = vi.fn()
      subscribeFenced('t', onInvalidate)
      expect(onInvalidate).toHaveBeenCalledTimes(1)
    }))

  it('T1.E2 negative — non-matching tag', () =>
    inRequest(async () => {
      await stampRequestStartFence()
      await getTagHub().publish(['other'])
      const onInvalidate = vi.fn()
      subscribeFenced('t', onInvalidate)
      expect(onInvalidate).toHaveBeenCalledTimes(0)
    }))

  it('T1.E3 negative — tag published before the fence', () =>
    inRequest(async () => {
      await getTagHub().ready()
      await getTagHub().publish(['t']) // observed BEFORE the fence stamp
      await stampRequestStartFence()
      const onInvalidate = vi.fn()
      subscribeFenced('t', onInvalidate)
      expect(onInvalidate).toHaveBeenCalledTimes(0)
    }))

  it('T1.E4/E15 journal overflow → unconditional replay', () =>
    inRequest(async () => {
      await stampRequestStartFence()
      for (let i = 0; i < 1030; i++) await getTagHub().publish(['t'])
      const onInvalidate = vi.fn()
      subscribeFenced('t', onInvalidate)
      expect(onInvalidate).toHaveBeenCalledTimes(1)
    }))

  it('T1.E5 a post-subscribe publish fires via the index; non-matching does not', () =>
    inRequest(async () => {
      await stampRequestStartFence()
      const onInvalidate = vi.fn()
      subscribeFenced('t', onInvalidate)
      expect(onInvalidate).toHaveBeenCalledTimes(0)
      await getTagHub().publish(['t'])
      expect(onInvalidate).toHaveBeenCalledTimes(1)
      await getTagHub().publish(['other'])
      expect(onInvalidate).toHaveBeenCalledTimes(1)
    }))

  it('T1.E11 attach ordering — a post-subscribe publish makes the source emit exactly once', () =>
    inRequest(async () => {
      await stampRequestStartFence()
      const onInvalidate = vi.fn()
      subscribeFenced('t', onInvalidate) // subscribed, no publish yet
      expect(onInvalidate).toHaveBeenCalledTimes(0)
      await getTagHub().publish(['t'])
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

describe('tag publish', () => {
  it('invalidate publishes immediately — it does not wait for the request to settle', () =>
    inRequest(async () => {
      await stampRequestStartFence()
      const publishSpy = vi.spyOn(getBroadcastAdapter(), 'publish')
      LiveCell.invalidate('t')
      LiveCell.invalidate('u')
      await flush()
      // The caller decides when a change is real; saying so publishes it. Holding invalidations until
      // the request settles would be this layer guessing at transaction boundaries it cannot see —
      // that belongs to whoever owns the write.
      const tags = tagBatchCalls(publishSpy).map((call) => (parse(call[1] as string) as { tags: string[] }).tags)
      expect(new Set(tags.flat())).toEqual(new Set(['t', 'u']))
    }))
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
      const onInvalidate = vi.fn()
      subscribeFenced('t', onInvalidate)
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
  it('T1.D2/J4 publish failure is logged + fired locally, result unmasked', () =>
    inRequest(async () => {
      await stampRequestStartFence() // barrier established over the working in-memory adapter
      const localListener = vi.fn()
      getTagHub().registerTag('t', localListener) // an external local subscriber
      // Only the settle publish fails (transport down at write time), not readiness:
      vi.spyOn(getBroadcastAdapter(), 'publish').mockRejectedValue(new Error('transport down'))
      const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
      LiveCell.invalidate('t') // fire-and-forget: a transport failure must never surface to the caller
      await flush()
      expect(localListener).toHaveBeenCalledTimes(1) // fired locally despite the failure
      expect(errorSpy).toHaveBeenCalled()
      errorSpy.mockRestore()
    }))
})

describe('a tag that could not be broadcast still reaches the requests that need it', () => {
  // A transport we can take down mid-test. While healthy, `send` loops the frame straight back to the
  // listener (like the in-memory adapter) so the readiness barrier can confirm. Once down, `send`
  // rejects and allocates NO backend seq — which is precisely what lets another instance's healthy
  // publish arrive carrying a seq this instance already spent on a local fallback.
  function controllableTransport() {
    let sendsFail = false
    let backendSeq = 0
    const listeners = new Map<string, (payload: string, info: { seq: number; timestamp: number }) => void>()
    const transport: BroadcastTransport = {
      send: (key, payload) => {
        if (sendsFail) return Promise.reject(new Error('broadcast transport is down'))
        const info = { seq: ++backendSeq, timestamp: 0 }
        listeners.get(key)?.(payload, info)
        return Promise.resolve(info)
      },
      listen: (key, onMessage) => {
        listeners.set(key, onMessage)
        return () => listeners.delete(key)
      },
      sendBinary: () => Promise.resolve({ seq: 0, timestamp: 0 }),
      listenBinary: () => () => {},
    }
    return {
      transport,
      takeDown: () => {
        sendsFail = true
      },
      /** A batch arriving from ANOTHER instance, carrying the backend's authoritative seq. */
      deliverFromPeer: (tags: string[], seq: number) => {
        for (const [key, onMessage] of listeners) onMessage(stringify({ tags }), { seq, timestamp: 0 })
      },
    }
  }

  /** Drive the REAL failure path rather than calling the fallback directly: invalidate publishes
   *  immediately, the downed transport rejects, and the hub falls back to firing local subscribers.
   *  The publish is fire-and-forget, so let it settle. */
  async function publishWhileDown(tag: string): Promise<void> {
    LiveCell.invalidate(tag)
    await flush()
  }

  let broadcast: ReturnType<typeof controllableTransport>
  let errorSpy: ReturnType<typeof vi.spyOn>
  beforeEach(async () => {
    broadcast = controllableTransport()
    _resetBroadcastAdapterForTesting(new DefaultBroadcastAdapter(broadcast.transport))
    _resetTagHubsForTesting()
    await getTagHub().ready() // subscribe + confirm the barrier while the transport is still healthy
    errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {}) // the fallback logs by design
  })
  afterEach(() => errorSpy.mockRestore())

  it('a publish that fails is still caught by a request that read before it and subscribed after', () =>
    inRequest(async () => {
      await stampRequestStartFence() // the request reads at this fence
      const fence = captureTagFence('t')

      broadcast.takeDown()
      await publishWhileDown('t') // a write elsewhere publishes 't'; the transport is down

      // The request subscribes at serialize, AFTER the failed publish fired locally. If the fallback
      // never entered the journal there is nothing for the catch-up scan to find, and this client
      // would hold its pre-write snapshot forever with no signal that it went stale.
      const onInvalidate = vi.fn()
      subscribeCapturedTag(fence, onInvalidate)
      expect(onInvalidate).toHaveBeenCalledTimes(1)
    }))

  it("a local fallback does not shadow another instance's real event at the same backend seq", async () => {
    broadcast.deliverFromPeer(['warmup'], 42) // this instance has observed the backend up to 42
    broadcast.takeDown()
    await publishWhileDown('x') // our publish fails → fires locally, spending an observation

    await inRequest(async () => {
      await stampRequestStartFence() // the request fences AFTER the local fallback
      const fence = captureTagFence('y')

      // Instance B was healthy: its publish of 'y' carries backend seq 43 — the number our fallback
      // already consumed locally. Adopting the transport's seq here would land this event AT the
      // fence, and the strictly-greater catch-up scan would miss it entirely.
      broadcast.deliverFromPeer(['y'], 43)

      const onInvalidate = vi.fn()
      subscribeCapturedTag(fence, onInvalidate)
      expect(onInvalidate).toHaveBeenCalledTimes(1)
    })
  })

  it('many local fallbacks do not suppress the real events that follow them', async () => {
    broadcast.deliverFromPeer(['warmup'], 42)
    broadcast.takeDown()
    for (let i = 0; i < 8; i++) await publishWhileDown(`local-${i}`) // inflate this instance's clock

    await inRequest(async () => {
      await stampRequestStartFence()
      const fence = captureTagFence('y')
      // Each of these carries a backend seq below the inflated local clock. Under a
      // transport-authoritative clock the whole run 43..50 would be silently suppressed — the window
      // spans many real deliveries, not just one recovery instant.
      for (let seq = 43; seq <= 50; seq++) broadcast.deliverFromPeer(['y'], seq)
      const onInvalidate = vi.fn()
      subscribeCapturedTag(fence, onInvalidate)
      expect(onInvalidate).toHaveBeenCalledTimes(1)
    })
  })

  it('the observation clock never goes backwards, whatever seq the transport reports', async () => {
    const hub = getTagHub()
    broadcast.takeDown()
    const observed: number[] = []
    for (let i = 0; i < 40; i++) {
      if (i % 2 === 0) await publishWhileDown(`t-${i}`)
      else broadcast.deliverFromPeer([`t-${i}`], 100 - i) // regressing backend seqs
      observed.push(hub.currentSeq())
    }
    // Every journal entry is stamped with the clock at append time, so a strictly increasing clock is
    // what keeps the journal ascending — and an ascending journal is what keeps the eviction
    // watermark, and with it the overflow backstop, meaningful.
    const strictlyAscending = observed.every((seq, i) => i === 0 || seq > observed[i - 1]!)
    expect(strictlyAscending).toBe(true)
  })
})
