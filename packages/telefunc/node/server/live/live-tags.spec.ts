import '../context/async.js' // keep the request context alive across the specs' macrotask awaits
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'
import { restoreContext } from '../context/context.js'
import { createStreamingReplacer } from '../../../wire-protocol/server/response/registry.js'
import type { ServerReplacerContext } from '../../../wire-protocol/types.js'
import { LiveCell } from './live.js'
import { stampRequestStartFence } from './tags.js'
import { getTagHub, _resetTagHubsForTesting } from './tagHub.js'
import {
  getBroadcastAdapter,
  _resetBroadcastAdapterForTesting,
  DefaultBroadcastAdapter,
} from '../../../wire-protocol/server/broadcast.js'

// A macrotask flush drains the tag-delivery + coalesced-emit chain (hub notify → live.invalidate →
// the cell's coalesced flush → channel send).
const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

let previousAdapter: ReturnType<typeof getBroadcastAdapter>
beforeEach(() => {
  previousAdapter = getBroadcastAdapter()
  _resetBroadcastAdapterForTesting(new DefaultBroadcastAdapter())
  _resetTagHubsForTesting()
})
afterEach(() => _resetBroadcastAdapterForTesting(previousAdapter))

function inRequest<T>(fn: () => Promise<T>): Promise<T> {
  return restoreContext({}, fn)
}

/** Publish calls carrying a tag batch (readiness-barrier frames are `{"barrier":…}`, no tags). */
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

describe('Live tag statics — LiveCell.onInvalidate / LiveCell.invalidate over TagHub (§3.C)', () => {
  it('T12.C1 LiveCell.onInvalidate binds the handle: a published tag → the handle channel invalidates', () =>
    inRequest(async () => {
      await stampRequestStartFence()
      const live = new LiveCell('rows')
      LiveCell.onInvalidate('t', live)
      const server = createServerHarness()
      server.serialize(live) // serialization activates the fenced tag source
      const channel = server.created[0]!
      await getTagHub().publish(['t'])
      await flush()
      expect(channel.sends).toEqual([{ kind: 'invalidate' }])
    }))

  it('T12.C2 fence catch-up: a tag published between the read and serialization is still caught', () =>
    inRequest(async () => {
      await stampRequestStartFence()
      const live = new LiveCell('rows')
      LiveCell.onInvalidate('t', live)
      await getTagHub().publish(['t']) // published BEFORE serialization — the read→attach window
      const server = createServerHarness()
      server.serialize(live) // the activation's scanSince catch-up delivers the earlier publish
      await flush()
      expect(server.created[0]!.sends).toEqual([{ kind: 'invalidate' }])
    }))

  it("T12.C1 a request's OWN LiveCell.invalidate self-invalidates its handle (harmless — invalidation is idempotent)", () =>
    inRequest(async () => {
      await stampRequestStartFence()
      const live = new LiveCell('rows')
      LiveCell.onInvalidate('t', live) // the handle is live under 't'...
      LiveCell.invalidate('t') // ...and the SAME request invalidates 't'
      await flush() // the publish is fire-and-forget; let it land
      const server = createServerHarness()
      server.serialize(live) // activation's fence catch-up sees 't' published since the request-start fence
      await flush()
      // Own-echo suppression was removed: a self-refetch is harmless (the client re-reads once), and can be
      // necessary if a write followed the returned read. So the handle fires — invalidation is idempotent.
      expect(server.created[0]!.sends).toEqual([{ kind: 'invalidate' }])
    }))

  it('T12.C3 LiveCell.invalidate out-of-request publishes immediately (void)', async () => {
    await getTagHub().ready()
    const publishSpy = vi.spyOn(getBroadcastAdapter(), 'publish')
    const result = LiveCell.invalidate('t')
    expect(result).toBeUndefined() // fire-and-forget
    await flush()
    expect(tagBatchCalls(publishSpy)).toHaveLength(1) // published immediately, not queued
  })

  it('never-serialized tag handle activates nothing (leak-safe)', () =>
    inRequest(async () => {
      await stampRequestStartFence()
      const live = new LiveCell('rows')
      LiveCell.onInvalidate('t', live)
      // Not serialized → the fenced source is never subscribed, so the publish below reaches nothing.
      const onInvalidate = vi.fn()
      live.onInvalidate(onInvalidate)
      await getTagHub().publish(['t'])
      await flush()
      expect(onInvalidate).not.toHaveBeenCalled() // nothing was subscribed to the tag
    }))
})
