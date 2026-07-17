import { describe, expect, it, vi } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { createStreamingReplacer } from './server/response/registry.js'
import { createStreamingReviver } from './client/response/registry.js'
import { LiveCell } from '../node/server/live/live.js'
import type { Live } from '../node/server/live/live.js'
import type { ClientReviverContext, ServerReplacerContext } from './types.js'

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

describe('Live wire replacer/reviver + serialize-time single activation (§3.D)', () => {
  it('T12.D1 a returned Live → {data, channelId} on the wire → revives to a Live with .data', () => {
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

  it('T12.D1 producer verbs after serialization ride the channel (coalesced data + invalidate events)', async () => {
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

  it('T12.A5 the revived consumer .data tracks pushes; onData/onInvalidate observe channel events', () => {
    const server = createServerHarness()
    const live = new LiveCell<string>('a')
    const body = server.serialize(live)
    const client = createClientHarness()
    const revived = client.parseBody(body) as Live<string>
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

  it('T12.D6 multi/nested walk: distinct handles register once; repeated refs revive to === identity', () => {
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
