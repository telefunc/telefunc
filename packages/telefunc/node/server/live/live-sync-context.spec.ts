// NO `import '../context/async.js'` here — this suite runs in telefunc's DEFAULT sync context mode (the
// PRODUCTION mode), where `restoreContext_sync` nulls the request context at the first macrotask
// (`setTimeout(…=null, 0)`). A real-I/O await (redis, fetch, …) is a macrotask, so any tag op AFTER such
// an await sees a null context. This is the mode `tags.spec.ts`/`live-tags.spec.ts` do NOT exercise
// (they import `context/async.js` = AsyncLocalStorage), which is why the docker regression slipped past.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'
import { restoreContext, getRawContext } from '../context/context.js'
import type { Context } from '../context/context.js'
import { createStreamingReplacer } from '../../../wire-protocol/server/response/registry.js'
import type { ServerReplacerContext } from '../../../wire-protocol/types.js'
import { LiveCell } from './live.js'
import { stampRequestStartFence, getRequestStartSeq } from './tags.js'
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

function createServerHarness(requestContext?: Context) {
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
    // Mirrors production: serialization takes the fence from the request context it was handed, not
    // from the ambient one — which in sync mode is already gone by the time this runs.
    get requestStartSeq() {
      return requestContext ? getRequestStartSeq(requestContext) : undefined
    },
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
  it('WRITE fix: LiveCell.invalidate after the await publishes immediately WITHOUT throwing', async () => {
    const publishSpy = vi.spyOn(getBroadcastAdapter(), 'publish')
    await restoreContext({}, async () => {
      await stampRequestStartFence()
      await macrotask() // context nulled
      expect(getRawContext()).toBeNull()
      expect(() => LiveCell.invalidate('t')).not.toThrow() // getRawContext() null → out-of-request immediate publish
    })
    await flush()
    expect(tagBatchCalls(publishSpy).length).toBeGreaterThanOrEqual(1) // the invalidation crossed the wire
  })

  it('associating a tag AFTER an await is valid — there is no ordering rule to get wrong', async () => {
    const requestContext = {}
    const server = createServerHarness(requestContext)
    const live = new LiveCell<string[]>([])
    await restoreContext(requestContext, async () => {
      await stampRequestStartFence()
      await macrotask() // a real-I/O await: the sync-mode context is nulled from here on
      expect(getRawContext()).toBeNull()
      // Associating a tag reads no context, so this is fine — which is the whole point. The fence was
      // stamped at request entry, and serialization resolves the key against it. Requiring this call to
      // precede the body's first await would be an ordering rule nothing enforces and every caller
      // would eventually break; a read above it would silently lose its catch-up.
      expect(() => LiveCell.onInvalidate('t', live)).not.toThrow()
      live.set(['fetched'])
      server.serialize(live)
    })
    await getTagHub().publish(['t'])
    await flush()
    expect(server.created[0]!.sends).toContainEqual({ kind: 'invalidate' })
  })

  it('LEAK-SAFETY: capture-but-never-serialize subscribes NOTHING (serialize-time single activation)', async () => {
    const invalidated = vi.fn()
    await restoreContext({}, async () => {
      await stampRequestStartFence()
      const live = new LiveCell<string[]>([])
      live.onInvalidate(invalidated) // observe whether ANY invalidation reaches the cell
      LiveCell.onInvalidate('t', live) // captures the fence, but the handle is NEVER serialized (no activate)
    })
    // Publishing the captured tag reaches nothing: capture alone subscribes no hub listener, so the
    // cell never invalidates. Were the fence subscribed eagerly at capture time — the leak this design
    // exists to prevent — this publish would deliver and the tap would fire.
    await getTagHub().publish(['t'])
    await flush()
    expect(invalidated).not.toHaveBeenCalled()
  })

  it('a tag published between the read and serialization is still caught, across a context-nulling await', async () => {
    const requestContext = {}
    const server = createServerHarness(requestContext)
    const live = new LiveCell<string[]>([])
    await restoreContext(requestContext, async () => {
      await stampRequestStartFence()
      LiveCell.onInvalidate('t', live)
      await macrotask() // the read's I/O await — the context is gone from here on
      // A write lands in the window between this request's read and its serialize. Catching it is the
      // entire reason the fence exists, and it must survive the context being nulled: serialization
      // resolves the tag against the explicitly-carried fence, never an ambient lookup.
      await getTagHub().publish(['t'])
      live.set(['fetched'])
      server.serialize(live)
    })
    await flush()
    expect(server.created[0]!.sends).toContainEqual({ kind: 'invalidate' })
  })
})
