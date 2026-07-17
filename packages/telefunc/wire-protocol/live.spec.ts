import { describe, expect, it, vi } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { createStreamingReplacer } from './server/response/registry.js'
import { createStreamingReviver } from './client/response/registry.js'
import { LiveCell } from '../node/server/live/live.js'
import type { Live, LiveSubscription } from '../node/server/live/live.js'
import type { ClientReviverContext, ServerReplacerContext } from './types.js'
import type { LiveEvent } from '../node/server/live/live.js'
import { ServerChannel } from './server/channel.js'
import { IndexedPeer } from './server/IndexedPeer.js'
import { ReplayBuffer } from './replay-buffer.js'
import { TAG, decode } from './shared-ws.js'

// Deterministic microtask flush — the cell's producer emissions are coalesced with `queueMicrotask`.
const tick = () => new Promise<void>((resolve) => queueMicrotask(() => resolve()))

type FakeServerChannel = {
  id: string
  sends: unknown[]
  fireClose: () => void
}
function makeFakeServerChannel(id: string): FakeServerChannel & Record<string, unknown> {
  const sends: unknown[] = []
  const closeCbs: Array<() => void> = []
  const openCbs: Array<() => void> = []
  return {
    id,
    sends,
    fireClose: () => closeCbs.forEach((cb) => cb()),
    send: (event: unknown) => {
      sends.push(event)
      return Promise.resolve()
    },
    onClose: (cb: () => void) => closeCbs.push(cb),
    onOpen: (cb: () => void) => openCbs.push(cb),
    close: () => Promise.resolve(),
    abort: () => {},
  }
}

function createServerHarness() {
  const created: FakeServerChannel[] = []
  let n = 0
  const context = {
    createChannel: () => {
      const channel = makeFakeServerChannel(`live-${n++}`)
      created.push(channel)
      return channel as never
    },
    registerChannel: () => {},
    sendStream: () => ({ metadata: { __index: 0 }, close() {}, abort() {} }),
    validators: new Map(),
    // Production always carries the fence, stamped at request entry; these fakes stand in for that.
    requestStartSeq: 0,
  } as unknown as ServerReplacerContext
  const replacer = createStreamingReplacer(
    () => context,
    () => {},
    [],
  )
  const serialize = (value: unknown) => stringify(value, { forbidReactElements: true, replacer })
  return { serialize, created }
}

type FakeClientChannel = { channelId: string; deliver: (event: unknown) => void }
function makeFakeClientChannel(channelId: string): FakeClientChannel & Record<string, unknown> {
  let listener: ((event: unknown) => void) | undefined
  const closeCbs: Array<() => void> = []
  return {
    channelId,
    isClosed: false,
    listen: (cb: (event: unknown) => void) => {
      listener = cb
      return () => {}
    },
    onClose: (cb: () => void) => closeCbs.push(cb),
    close: () => Promise.resolve(0),
    abort: () => {},
    deliver: (event: unknown) => listener?.(event),
  }
}

function createClientHarness() {
  const minted: FakeClientChannel[] = []
  const context = {
    createChannel: (opts: { channelId: string }) => {
      const channel = makeFakeClientChannel(opts.channelId)
      minted.push(channel)
      return channel as never
    },
    createBroadcast: () => ({}) as never,
    receiveStream: () => {
      throw new Error('registry-level harness does not stream')
    },
    waitFor: () => {},
  } as unknown as ClientReviverContext
  const reviver = createStreamingReviver(context, () => {}, [])
  const parseBody = (body: string) => parse(body, { reviver }) as Live<unknown>
  return { parseBody, minted }
}

describe('Live wire replacer/reviver + serialize-time single activation', () => {
  it('a returned Live → {data, channelId} on the wire → revives to a Live with .data', () => {
    const server = createServerHarness()
    const live = new LiveCell({ n: 1 })
    const body = server.serialize(live)
    expect(body).toContain('!TelefuncLive:')
    expect(server.created).toHaveLength(1) // the channel is created AT serialization

    const client = createClientHarness()
    const revived = client.parseBody(body)
    expect(client.minted).toHaveLength(1)
    expect(client.minted[0]!.channelId).toBe(server.created[0]!.id)
    expect(revived.data).toEqual({ n: 1 }) // .data seeds from the wire snapshot (SSR .data-only path)
  })

  it('never-serialized activates nothing — no channel created unless a Live crosses the wire', () => {
    const server = createServerHarness()
    server.serialize({ plain: 'x', n: 42 }) // no Live in the value
    expect(server.created).toHaveLength(0)
    // A Live that exists but is not part of the serialized value activates nothing either.
    const live = new LiveCell('v')
    server.serialize({ other: 'y' })
    expect(server.created).toHaveLength(0)
    // Only when the Live actually crosses the wire is a channel created — exactly once.
    server.serialize(live)
    expect(server.created).toHaveLength(1)
  })

  it('producer verbs after serialization ride the channel (coalesced data + invalidate events)', async () => {
    const server = createServerHarness()
    const live = new LiveCell('a')
    server.serialize(live)
    const channel = server.created[0]!
    live.set('b')
    live.set('c') // coalesced → one data event with the last value
    await tick()
    expect(channel.sends).toEqual([{ kind: 'data', data: 'c' }])
    live.invalidate()
    await tick()
    expect(channel.sends).toEqual([{ kind: 'data', data: 'c' }, { kind: 'invalidate' }])
  })

  it('the revived consumer .data tracks pushes; onData/onInvalidate observe channel events', () => {
    const server = createServerHarness()
    const live = new LiveCell<string>('a')
    const body = server.serialize(live)
    const client = createClientHarness()
    // The revived handle is publicly a `Live<T>`, but this test binds the taps an adapter uses — so it
    // is the adapter's view, `Live & LiveSubscription`. Claiming plain `Live<T>` and then calling
    // methods it does not have would describe a handle that could not exist.
    const revived = client.parseBody(body) as Live<string> & LiveSubscription<string>
    const channel = client.minted[0]!

    const onData = vi.fn()
    const onInvalidate = vi.fn()
    revived.onData(onData)
    revived.onInvalidate(onInvalidate)

    channel.deliver({ kind: 'data', data: 'z' })
    expect(revived.data).toBe('z') // .data updates BEFORE the tap fires
    expect(onData).toHaveBeenCalledWith('z')

    channel.deliver({ kind: 'invalidate' })
    expect(onInvalidate).toHaveBeenCalledTimes(1)
    expect(revived.data).toBe('z') // an invalidate leaves .data unchanged
  })

  it('multi/nested walk: distinct handles register once; repeated refs revive to === identity', () => {
    const server = createServerHarness()
    const todos = new LiveCell([1, 2])
    const report = new LiveCell('R')
    // Two distinct handles plus repeated references nested across objects/arrays.
    const value = {
      todos: todos,
      report: report,
      dup: todos,
      list: [report, { deep: todos }],
    }
    const body = server.serialize(value)
    expect(server.created).toHaveLength(2) // one channel per DISTINCT handle (repeats do not re-create)

    const client = createClientHarness()
    const revived = client.parseBody(body) as unknown as {
      todos: Live<unknown>
      report: Live<unknown>
      dup: Live<unknown>
      list: [Live<unknown>, { deep: Live<unknown> }]
    }
    expect(client.minted).toHaveLength(2) // two distinct client objects
    expect(revived.todos).toBe(revived.dup) // repeated ref → same revived object
    expect(revived.todos).toBe(revived.list[1].deep)
    expect(revived.report).toBe(revived.list[0])
    expect(revived.todos).not.toBe(revived.report) // distinct handles stay distinct
  })
})

// The full chain over a REAL ServerChannel: replacer → real pre-peer buffer → real peer attach.
// A fake channel here would only re-prove the buffer's own unit test.
function createRealChannelHarness(bufferLimit: number) {
  const channel = new ServerChannel<never, LiveEvent<unknown>>({ id: crypto.randomUUID(), bufferLimit })
  const context = {
    createChannel: () => channel,
    registerChannel: () => {},
    sendStream: () => ({ metadata: { __index: 0 }, close() {}, abort() {} }),
    validators: new Map(),
    requestStartSeq: 0,
  } as unknown as ServerReplacerContext
  const replacer = createStreamingReplacer(
    () => context,
    () => {},
    [],
  )
  return { channel, serialize: (v: unknown) => stringify(v, { forbidReactElements: true, replacer }) }
}

/** Attach a real client peer and collect the frames it receives. */
function attachPeer(channel: ServerChannel<never, LiveEvent<unknown>>) {
  const frames: Uint8Array[] = []
  channel._attachPeer(
    new IndexedPeer({ send: (frame) => void frames.push(frame) }, 7, new ReplayBuffer(1 << 20, 60_000, 1 << 21)),
  )
  return frames.map((f) => decode(f)).filter((d) => d.tag === TAG.TEXT || d.tag === TAG.PUBLISH)
}

describe('a live query whose pre-peer frames are dropped still reaches the client', () => {
  it('an invalidation dropped from the pre-peer buffer is still delivered on connect', async () => {
    // A buffer too small to hold even one invalidate frame, so that frame is dropped outright — and
    // NOTHING else is emitted. Any other traffic here would independently mark the client as missing
    // something, and this test would pass without ever exercising the invalidation path it names.
    const { channel, serialize } = createRealChannelHarness(8)
    const live = new LiveCell<string>('initial')
    serialize(live)

    live.invalidate() // the client is not connected yet, and this frame does not survive the buffer
    await tick()

    const received = attachPeer(channel)
    // The buffer has nothing left to flush, so the repair is the only thing that can tell this client
    // its data moved on. Without it, the client keeps `initial` forever.
    expect(received.some((d) => 'text' in d && d.text.includes('invalidate'))).toBe(true)
  })

  it('a data push evicted before connect is not lost silently either', async () => {
    const { channel, serialize } = createRealChannelHarness(64)
    const live = new LiveCell<string>('initial')
    serialize(live)

    live.set('x'.repeat(500)) // the ONLY emission, and it is dropped outright by the oversized path
    await tick()

    const received = attachPeer(channel)
    // A Live's truth can travel in `data` frames just as much as in invalidations. The client cannot be
    // left on the wire snapshot believing it is current, so it is told to refetch.
    expect(received.some((d) => 'text' in d && d.text.includes('invalidate'))).toBe(true)
  })
})

describe('the refetch-on-connect repair must not fire for data the client already has', () => {
  it('a value set before serialization does not trigger a redundant refetch', async () => {
    const { channel, serialize } = createRealChannelHarness(4096) // roomy: nothing is evicted here
    const live = new LiveCell<string>('initial')
    // The producer populates the cell BEFORE returning it — the shape any `live.set(...)` producer has.
    // The wire snapshot therefore already carries this value.
    live.set('populated')
    serialize(live)
    await tick() // the coalesced data emission lands here: after serialize, before the client connects

    const received = attachPeer(channel)
    // Telling this client to refetch would be telling it to fetch what it already has — and the refetch
    // would build another Live the same way, populate it the same way, and be told to refetch again.
    expect(received.some((d) => 'text' in d && d.text.includes('invalidate'))).toBe(false)
  })
})

describe('a Live cannot be serialized without the request fence', () => {
  it('a missing fence fails loudly instead of quietly starting from now', () => {
    const context = {
      createChannel: () => makeFakeServerChannel('x') as never,
      registerChannel: () => {},
      sendStream: () => ({ metadata: { __index: 0 }, close() {}, abort() {} }),
      validators: new Map(),
      // No requestStartSeq: the plumbing that carries it is broken.
    } as unknown as ServerReplacerContext
    const replacer = createStreamingReplacer(
      () => context,
      () => {},
      [],
    )
    // Defaulting to "observe from now" here would look like success and lose every write that landed
    // between this request's read and this moment — silently, which is the one outcome the fence
    // exists to prevent. Serialization is always a telefunction response, so an absent fence is a bug
    // in us, not a mode to accommodate.
    expect(() => stringify(new LiveCell('v'), { forbidReactElements: true, replacer })).toThrow()
  })
})
