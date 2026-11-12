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
