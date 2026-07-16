import { describe, expect, it, vi } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { createStreamingReplacer } from './server/response/registry.js'
import { createStreamingReviver } from './client/response/registry.js'
import { Live } from '../node/server/live/live.js'
import type { ClientLive } from '../node/server/live/live.js'
import type { ClientReviverContext, ServerReplacerContext } from './types.js'

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
  const parseBody = (body: string) => parse(body, { reviver }) as ClientLive<unknown>
  return { parseBody, minted }
}

describe('Live wire replacer/reviver + serialize-time single activation (§3.D)', () => {
  it('T12.D1 a returned ClientLive → {data, channelId} on the wire → revives to a ClientLive with .data', () => {
    const server = createServerHarness()
    const live = new Live({ n: 1 })
    const body = server.serialize(live.client)
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
    const live = new Live('v')
    server.serialize({ other: 'y' })
    expect(server.created).toHaveLength(0)
    // Only when the Live actually crosses the wire is a channel created — exactly once.
    server.serialize(live.client)
    expect(server.created).toHaveLength(1)
  })

  it('T12.A5 the revived consumer observes invalidation; .data stays the wire snapshot (invalidation-only)', () => {
    const server = createServerHarness()
    const live = new Live<string>('a')
    const body = server.serialize(live.client)
    const client = createClientHarness()
    const revived = client.parseBody(body) as ClientLive<string>
    const channel = client.minted[0]!

    const onInvalidate = vi.fn()
    revived.onInvalidate(onInvalidate)

    channel.deliver({ kind: 'invalidate' })
    expect(onInvalidate).toHaveBeenCalledTimes(1)
    expect(revived.data).toBe('a') // .data is the fixed snapshot — an invalidate just triggers a client refetch
  })

  it('T12.D6 multi/nested walk: distinct handles register once; repeated refs revive to === identity', () => {
    const server = createServerHarness()
    const todos = new Live([1, 2])
    const report = new Live('R')
    // Two distinct handles plus repeated references nested across objects/arrays.
    const value = {
      todos: todos.client,
      report: report.client,
      dup: todos.client,
      list: [report.client, { deep: todos.client }],
    }
    const body = server.serialize(value)
    expect(server.created).toHaveLength(2) // one channel per DISTINCT handle (repeats do not re-create)

    const client = createClientHarness()
    const revived = client.parseBody(body) as unknown as {
      todos: ClientLive<unknown>
      report: ClientLive<unknown>
      dup: ClientLive<unknown>
      list: [ClientLive<unknown>, { deep: ClientLive<unknown> }]
    }
    expect(client.minted).toHaveLength(2) // two distinct client objects
    expect(revived.todos).toBe(revived.dup) // repeated ref → same revived object
    expect(revived.todos).toBe(revived.list[1].deep)
    expect(revived.report).toBe(revived.list[0])
    expect(revived.todos).not.toBe(revived.report) // distinct handles stay distinct
  })
})
