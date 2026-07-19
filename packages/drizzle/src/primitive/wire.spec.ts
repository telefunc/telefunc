import { describe, expect, it, vi } from 'vitest'
import { LiveCell } from './live.js'
import { liveReplacer } from './wireServer.js'
import { liveReviver } from './wireClient.js'
import { SERIALIZER_PREFIX_LIVE } from './wireConstants.js'
import type { ServerReplacerContext } from 'telefunc'
import type { ClientReviverContext } from 'telefunc/client'

// THE WIRE CONTRACT, both halves, owned by this package.
//
// It used to run through telefunc's `createStreamingReplacer`/`createStreamingReviver` and the real
// json-serializer. Those are core internals this package no longer reaches, so the two halves are driven
// DIRECTLY here and joined by a fake channel pair. What that gives up is core's own object walk (does the
// serializer FIND a Live nested in a returned object?) — core's behaviour, not this package's, and proven
// end to end by the multi-instance browser e2e. What it keeps is everything that IS this package's: the
// metadata shape, activation timing, invalidation delivery, and that the two halves agree on the prefix.

// Deterministic microtask flush — the cell's producer emissions are coalesced with `queueMicrotask`.
const tick = () => new Promise<void>((resolve) => queueMicrotask(() => resolve()))

/** A fake channel pair: what the server sends, the client's listener receives. This IS the wire for the
 *  purposes of this spec — the real one is a telefunc channel, whose transport is core's concern. */
function channelPair(id: string) {
  const serverSends: unknown[] = []
  const closeCbs: Array<() => void> = []
  const listeners: Array<(event: unknown) => void> = []
  const server = {
    id,
    send: (event: unknown) => {
      serverSends.push(event)
      for (const listen of listeners) listen(event) // delivery to the client end
      return Promise.resolve()
    },
    onClose: (cb: () => void) => closeCbs.push(cb),
    close: () => Promise.resolve(),
    abort: () => {},
  }
  const client = {
    id,
    listen: (cb: (event: unknown) => void) => listeners.push(cb),
    close: () => Promise.resolve(),
    abort: () => {},
  }
  return { server, client, serverSends, fireClose: () => closeCbs.forEach((cb) => cb()) }
}

function harness() {
  const channels: ReturnType<typeof channelPair>[] = []
  let n = 0
  const serverContext = {
    createChannel: () => {
      const pair = channelPair(`ch-${n++}`)
      channels.push(pair)
      return pair.server as never
    },
    registerChannel: () => {},
    validators: new Map(),
  } as unknown as ServerReplacerContext
  const clientContext = {
    createChannel: ({ channelId }: { channelId: string }) =>
      channels.find((c) => c.server.id === channelId)!.client as never,
  } as unknown as ClientReviverContext
  return { serverContext, clientContext, channels }
}

describe('the Live wire — server replacer', () => {
  it('detects a cell and nothing else', () => {
    expect(liveReplacer.detect(new LiveCell(1))).toBe(true)
    expect(liveReplacer.detect({ data: 1 })).toBe(false) // shape alone is not the brand
    expect(liveReplacer.detect(null)).toBe(false)
    expect(liveReplacer.detect('x')).toBe(false)
  })

  it('a returned Live → {data, channelId} on the wire → revives to a Live with .data', () => {
    const { serverContext, clientContext } = harness()
    const cell = new LiveCell([{ id: 1 }])
    const { metadata } = liveReplacer.replace(cell, serverContext)
    expect(metadata).toEqual({ data: [{ id: 1 }], channelId: 'ch-0' })
    const revived = liveReviver.revive(metadata as { data: unknown; channelId: string }, clientContext)
    expect((revived.value as { data: unknown }).data).toEqual([{ id: 1 }])
  })

  it('never-serialized activates nothing — no channel and no source subscription', () => {
    const subscribe = vi.fn(() => () => {})
    const cell = new LiveCell('a')
    cell.attachSource({ subscribe })
    const { channels } = harness()
    // The cell exists and has a source, but nothing crossed the wire.
    expect(channels).toHaveLength(0)
    expect(subscribe).not.toHaveBeenCalled()
  })

  it('the invalidation tap is installed BEFORE activation — a source that replays on subscribe is not lost', () => {
    // The ordering bug this guards: activation subscribes the source, and a source that fires a catch-up
    // synchronously ON subscribe would emit with no tap attached — the client keeps a snapshot it should
    // have refetched. Asserted by making the source do exactly that.
    const { serverContext, channels } = harness()
    const cell = new LiveCell('a')
    cell.attachSource({
      subscribe: (onInvalidate) => {
        onInvalidate() // replay a catch-up synchronously, during activate()
        return () => {}
      },
    })
    liveReplacer.replace(cell, serverContext)
    return tick().then(() => {
      expect(channels[0]!.serverSends).toEqual([undefined]) // the replayed signal reached the wire
    })
  })

  it('an invalidation after serialization rides the channel (coalesced)', async () => {
    const { serverContext, channels } = harness()
    const cell = new LiveCell(1)
    liveReplacer.replace(cell, serverContext)
    cell.invalidate()
    cell.invalidate() // same microtask window → one delivery
    await tick()
    expect(channels[0]!.serverSends).toEqual([undefined])
  })

  it('the channel close releases the lease, and a closed cell stops emitting', async () => {
    const teardown = vi.fn()
    const { serverContext, channels } = harness()
    const cell = new LiveCell(1)
    cell.attachSource({ subscribe: () => teardown })
    liveReplacer.replace(cell, serverContext)
    channels[0]!.fireClose()
    expect(teardown).toHaveBeenCalledTimes(1) // last owning channel → source torn down
    cell.invalidate()
    await tick()
    expect(channels[0]!.serverSends).toEqual([]) // released → closed → never fires again
  })
})

describe('the Live wire — client reviver', () => {
  it('.data is the wire snapshot; onInvalidate observes channel events; unsubscribe is idempotent', async () => {
    const { serverContext, clientContext } = harness()
    const cell = new LiveCell('snapshot')
    const { metadata } = liveReplacer.replace(cell, serverContext)
    const handle = liveReviver.revive(metadata as { data: unknown; channelId: string }, clientContext).value as {
      data: unknown
      onInvalidate(cb: () => void): () => void
    }
    expect(handle.data).toBe('snapshot')

    const tap = vi.fn()
    const off = handle.onInvalidate(tap)
    cell.invalidate()
    await tick()
    expect(tap).toHaveBeenCalledTimes(1) // server → channel → client tap, end to end

    off()
    off() // idempotent
    cell.invalidate()
    await tick()
    expect(tap).toHaveBeenCalledTimes(1) // no longer observing
  })

  it('.data does NOT change in place — the primitive is invalidation-only', async () => {
    const { serverContext, clientContext } = harness()
    const cell = new LiveCell('first')
    const { metadata } = liveReplacer.replace(cell, serverContext)
    const handle = liveReviver.revive(metadata as { data: unknown; channelId: string }, clientContext).value as {
      data: unknown
    }
    cell.invalidate()
    await tick()
    expect(handle.data).toBe('first') // staleness drives a refetch; it never mutates the snapshot
  })
})

describe('the Live wire — the two halves agree', () => {
  it('replacer and reviver share ONE prefix constant', () => {
    // A drifting prefix is a silent wire break: the client simply never recognises the tag, and the
    // failure surfaces as "invalidation does nothing" rather than as an error. They read the same module.
    expect(liveReplacer.prefix).toBe(SERIALIZER_PREFIX_LIVE)
    expect(liveReviver.prefix).toBe(SERIALIZER_PREFIX_LIVE)
    expect(SERIALIZER_PREFIX_LIVE).toBe('!TelefuncLive:')
  })
})
