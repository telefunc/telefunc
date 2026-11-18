import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { BroadcastTransport } from 'telefunc'
import { ChangeFeed } from './changeFeed.js'
import type { Change } from './match/liveRead.js'

type Listener = (payload: string, info: { seq: number; timestamp: number }) => void

/** Two servers' transport, whose first `send()` fails before the message is sequenced, as a connection that broke. */
function failingOnce(failure: 'throws' | 'never settles') {
  const listeners = new Map<string, Set<Listener>>()
  let seq = 0
  let failed = false
  const transport: BroadcastTransport = {
    listen(key, listener) {
      if (!listeners.has(key)) listeners.set(key, new Set())
      listeners.get(key)!.add(listener)
      return () => listeners.get(key)!.delete(listener)
    },
    send(key, payload) {
      if (!failed) {
        failed = true
        if (failure === 'throws') throw new Error('connection lost')
        return new Promise(() => {})
      }
      const info = { seq: ++seq, timestamp: Date.now() }
      for (const listener of listeners.get(key) ?? []) listener(payload as string, info)
      return info
    },
    listenBinary: () => () => {},
    sendBinary: () => ({ seq: 0, timestamp: 0 }),
  } as unknown as BroadcastTransport
  return transport
}

/** A transport whose messages the test delivers, with the sequence numbers it picks, as another server sent them. */
function deliveredByTest() {
  const listeners = new Set<Listener>()
  const transport = {
    listen(_key: string, listener: Listener) {
      listeners.add(listener)
      return () => listeners.delete(listener)
    },
  } as unknown as BroadcastTransport
  const deliver = (seq: number, change: Change) => {
    const payload = JSON.stringify({ origin: 'another server', changes: [change] })
    for (const listener of listeners) listener(payload, { seq, timestamp: 0 })
  }
  return { transport, deliver }
}

const todo1: Change = {
  table: 'public.todos',
  kind: 'update',
  old: { id: '1', done: 'f' },
  new: { id: '1', done: 't' },
}
const todo2: Change = {
  table: 'public.todos',
  kind: 'update',
  old: { id: '2', done: 'f' },
  new: { id: '2', done: 't' },
}

describe('ChangeFeed', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.spyOn(console, 'error').mockImplementation(() => {})
  })
  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('delivers a message that arrives after a later one, and a repeat of one once', () => {
    const { transport, deliver } = deliveredByTest()
    const received: Change[] = []
    new ChangeFeed(transport, 'app', (changes) => received.push(...changes)).watch(['public.todos'])
    deliver(2, todo2)
    deliver(1, todo1)
    deliver(2, todo2)
    expect(received).toEqual([todo2, todo1])
  })

  it('takes a message too far behind the latest to tell from a repeat as a change to any row', () => {
    const { transport, deliver } = deliveredByTest()
    const received: Change[] = []
    new ChangeFeed(transport, 'app', (changes) => received.push(...changes)).watch(['public.todos'])
    deliver(100, todo2)
    deliver(37, todo1)
    deliver(36, todo1)
    expect(received).toEqual([todo2, todo1, { table: 'public.todos', kind: 'unknown' }])
  })

  it("waits for an app transport's listener that the broker confirms later, and listens again once it refuses one", async () => {
    const confirmations: { resolve: () => void; reject: (error: Error) => void }[] = []
    // Like a broker client that holds a key from `listen()` until `unsubscribe()` let it go, a tick later, and then failed.
    const held = new Set<string>()
    const releaseErrors = vi.spyOn(console, 'error').mockImplementation(() => {})
    const transport = {
      listen: (key: string) => {
        if (held.has(key)) return { unsubscribe: () => {}, ready: Promise.reject(new Error(`${key} is held`)) }
        held.add(key)
        const { promise, resolve, reject } = Promise.withResolvers<void>()
        confirmations.push({ resolve, reject })
        const unsubscribe = async () => {
          await Promise.resolve()
          held.delete(key)
          throw new Error('connection lost while releasing')
        }
        return { unsubscribe, ready: promise }
      },
    } as unknown as BroadcastTransport
    const feed = new ChangeFeed(transport, 'app', () => {})
    let confirmed = false
    void feed.watch(['public.todos'])!.then(() => (confirmed = true))
    await Promise.resolve()
    expect(confirmed).toBe(false)
    confirmations[0]!.resolve()
    await vi.waitFor(() => expect(confirmed).toBe(true))
    expect(feed.watch(['public.todos'])).toBeUndefined()

    const refused = feed.watch(['public.notes'])!
    confirmations[1]!.reject(new Error('refused'))
    await expect(refused).rejects.toThrow('refused')
    const again = feed.watch(['public.notes'])!
    confirmations[2]!.resolve()
    await expect(again).resolves.toBeUndefined()
    expect(releaseErrors).toHaveBeenCalledOnce()
    releaseErrors.mockRestore()
  })

  for (const failure of ['throws', 'never settles'] as const) {
    it(`sends a change again once its publish ${failure}`, async () => {
      const transport = failingOnce(failure)
      const received: Change[] = []
      const writer = new ChangeFeed(transport, 'app', () => {})
      const reader = new ChangeFeed(transport, 'app', (changes) => received.push(...changes))
      reader.watch(['public.todos'])
      writer.publish([todo1])
      writer.publish([todo2])
      await vi.advanceTimersByTimeAsync(20_000)
      expect(received).toEqual([todo1, todo2])
    })
  }
})
