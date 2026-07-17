// NO `import '../context/async.js'` here — this suite runs in telefunc's DEFAULT sync context mode (the
// PRODUCTION mode), where `restoreContext_sync` nulls the request context at the first macrotask
// (`setTimeout(…=null, 0)`). A real-I/O await (redis, fetch, …) is a macrotask, so any tag op AFTER such
// an await sees a null context. This is the mode `tags.spec.ts`/`live-tags.spec.ts` do NOT exercise
// (they import `context/async.js` = AsyncLocalStorage), which is why the docker regression slipped past.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'
import { restoreContext, getRawContext } from '../context/context.js'
import { createStreamingReplacer } from '../../../wire-protocol/server/response/registry.js'
import type { ServerReplacerContext } from '../../../wire-protocol/types.js'
import { Live } from './live.js'
import { _activationStateForTesting, _listenerCountForTesting } from './testing.js'
import { invalidateTag, stampRequestStartFence } from './tags.js'
import { getTagHub, _resetTagHubsForTesting } from './tagHub.js'
import {
  getBroadcastAdapter,
  _resetBroadcastAdapterForTesting,
  DefaultBroadcastAdapter,
} from '../../../wire-protocol/server/broadcast.js'

// A real macrotask — the same class of yield as `await redis.set()`. The sync-mode context-nulling
// `setTimeout(0)` (scheduled first, inside restoreContext) fires before this resolves, so the context is
// already null when execution resumes here.
const macrotask = () => new Promise<void>((resolve) => setTimeout(resolve, 0))
const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

let previousAdapter: ReturnType<typeof getBroadcastAdapter>
beforeEach(() => {
  previousAdapter = getBroadcastAdapter()
  _resetBroadcastAdapterForTesting(new DefaultBroadcastAdapter())
  _resetTagHubsForTesting()
})
afterEach(() => _resetBroadcastAdapterForTesting(previousAdapter))

function tagBatchCalls(spy: ReturnType<typeof vi.spyOn>): unknown[][] {
  return spy.mock.calls.filter((c) => typeof c[1] === 'string' && (c[1] as string).includes('"tags"'))
}

type FakeChannel = { id: string; sends: unknown[] }
function makeFakeChannel(id: string): FakeChannel & Record<string, unknown> {
  const sends: unknown[] = []
  return {
    id,
    sends,
    send: (event: unknown) => {
      sends.push(event)
      return Promise.resolve()
    },
    onClose: () => {},
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

describe('sync context mode — tag usage must survive a macrotask (real-I/O) await', () => {
  it('the mechanism: after a macrotask await the request context is null → OLD invalidateTag THROWS', async () => {
    let contextAfter: unknown = 'unset'
    let thrown: unknown
    await restoreContext({}, async () => {
      await stampRequestStartFence()
      await macrotask() // real-I/O await → sync context nulled (the redis-in-docker path)
      contextAfter = getRawContext()
      try {
        invalidateTag('t')
      } catch (err) {
        thrown = err
      }
    })
    expect(contextAfter).toBeNull() // proves the mechanism: the context is gone post-await
    expect(thrown).toBeInstanceOf(Error)
    expect(String(thrown)).toMatch(/inside a telefunction/) // the literal CI error
  })

  it('WRITE fix: Live.invalidate after the await publishes immediately WITHOUT throwing', async () => {
    const publishSpy = vi.spyOn(getBroadcastAdapter(), 'publish')
    await restoreContext({}, async () => {
      await stampRequestStartFence()
      await macrotask() // context nulled
      expect(getRawContext()).toBeNull()
      expect(() => Live.invalidate('t')).not.toThrow() // getRawContext() null → out-of-request immediate publish
    })
    await flush()
    expect(tagBatchCalls(publishSpy).length).toBeGreaterThanOrEqual(1) // the invalidation crossed the wire
  })

  it('READ fix (guard): Live.onInvalidate called AFTER the await THROWS — enforces subscribe-before-fetch', async () => {
    let thrown: unknown
    await restoreContext({}, async () => {
      await stampRequestStartFence()
      await macrotask() // context nulled
      const live = new Live<string[]>([])
      try {
        Live.onInvalidate('t', live) // eager fence capture needs the live context — throws post-await
      } catch (err) {
        thrown = err
      }
    })
    expect(thrown).toBeInstanceOf(Error)
    expect(String(thrown)).toMatch(/inside a telefunction/)
  })

  it('LEAK-SAFETY: capture-but-never-serialize registers NO hub listener (serialize-time single activation)', async () => {
    await restoreContext({}, async () => {
      await stampRequestStartFence()
      const live = new Live<string[]>([])
      Live.onInvalidate('t', live) // captures the fence, but the handle is NEVER serialized (no _activate)
      expect(_listenerCountForTesting(getTagHub(), 't')).toBe(0) // capture touched no hub listener
      expect(_activationStateForTesting(live).sourceActive).toBe(false) // and nothing activated on the cell
    })
    expect(_listenerCountForTesting(getTagHub(), 't')).toBe(0) // still zero after the request — nothing to leak
  })

  it('READ fix: Live.onInvalidate BEFORE the await + live.set after → the serialized handle fires on a later publish', async () => {
    const server = createServerHarness()
    await restoreContext({}, async () => {
      await stampRequestStartFence()
      const live = new Live<string[]>([])
      Live.onInvalidate('t', live) // BEFORE any await — fence captured while the context is live
      await macrotask() // context nulled (the redis await)
      live.set(['fetched']) // handle op after the await — no context needed
      server.serialize(live.client) // serialize post-await (context null) — must use the CAPTURED fence
    })
    const channel = server.created[0]!
    await getTagHub().publish(['t'])
    await flush()
    // The handle is live across the context-nulling await: a later cross-request publish reaches it. (The
    // pre-serialize `live.set` may also ride an initial data frame — harness-timing-dependent — so assert
    // the invalidate is delivered rather than the exact frame sequence.)
    expect(channel.sends).toContainEqual({ kind: 'invalidate' })
  })
})
