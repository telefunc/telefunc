import { AsyncLocalStorage } from 'node:async_hooks'
import { afterEach, beforeAll, describe, expect, it } from 'vitest'
import { Broadcast, ServerBroadcast } from './ServerBroadcast.js'
import { disposeBackend } from '../../backend/install.js'
import { getContext, PROVIDED_CONTEXT } from '../../../node/server/context/getContext.js'
import { getRawContext, restoreContext } from '../../../node/server/context/context.js'
import { installAsyncContext } from '../../../node/server/context/async.js'
import type { Unsubscribe } from '../channel.js'
import { onBug } from '../../../node/server/runTelefunc/onBug.js'

afterEach(() => disposeBackend())

/** Whose context `onBug()` was called in, for each bug while a test listens. */
let bugContexts: string[] | null = null
onBug(() => bugContexts?.push(whoseContext()))
afterEach(() => {
  bugContexts = null
})

/** Whose context `getContext()` gives where it's called, or that it throws. */
function whoseContext(): string {
  try {
    return getContext<{ user: string }>().user
  } catch {
    return 'throws'
  }
}

function asUser<T>(user: string, fn: () => T): T {
  return restoreContext({ [PROVIDED_CONTEXT]: { user } }, fn)
}

/** Each server-side subscriber of a key, and a publish it receives. */
const subscribers: Array<{
  name: string
  subscribe: (key: string, listener: () => void) => Unsubscribe
  publish: (key: string) => unknown
}> = [
  {
    name: "a BroadcastChannel's subscribe()",
    subscribe: (key, listener) => new ServerBroadcast<string>({ key }).subscribe(listener),
    publish: (key) => Broadcast.publish(key, 'hello'),
  },
  {
    name: "a BroadcastChannel's subscribeBinary()",
    subscribe: (key, listener) => new ServerBroadcast<string>({ key }).subscribeBinary(listener),
    publish: (key) => Broadcast.publishBinary(key, new Uint8Array([1])),
  },
  {
    name: 'Broadcast.subscribe()',
    subscribe: (key, listener) => Broadcast.subscribe(key, listener),
    publish: (key) => Broadcast.publish(key, 'hello'),
  },
  {
    name: 'Broadcast.subscribeBinary()',
    subscribe: (key, listener) => Broadcast.subscribeBinary(key, listener),
    publish: (key) => Broadcast.publishBinary(key, new Uint8Array([1])),
  },
]

function itGivesNoContext() {
  for (const { name, subscribe, publish } of subscribers)
    it(`${name}'s listener, subscribed in alice's context, gets no context for bob's publish or one in no context`, async () => {
      const key = `context:${crypto.randomUUID()}`
      const read: string[] = []
      const unsubscribe = asUser('alice', () => subscribe(key, () => read.push(whoseContext())))
      await unsubscribe.opened
      await asUser('bob', () => publish(key))
      // Without AsyncLocalStorage, a context lasts until the next macrotask.
      await new Promise((resolve) => setTimeout(resolve, 0))
      await publish(key)
      expect(read).toEqual(['throws', 'throws'])
      unsubscribe()
    })
  for (const { name, subscribe, publish } of subscribers)
    it(`${name}'s listener that throws on bob's publish is reported to onBug() with no context`, async () => {
      const key = `context:${crypto.randomUUID()}`
      const unsubscribe = subscribe(key, () => {
        throw new Error('listener bug')
      })
      await unsubscribe.opened
      bugContexts = []
      await asUser('bob', () => publish(key))
      expect(bugContexts).toEqual(['throws'])
      unsubscribe()
    })
}

describe('without AsyncLocalStorage', () => {
  itGivesNoContext()
})

// Last in its file: AsyncLocalStorage, once installed, stays for the file's tests that come after it.
describe('with AsyncLocalStorage', () => {
  beforeAll(() => installAsyncContext(AsyncLocalStorage))
  itGivesNoContext()

  for (const kind of ['text', 'binary'] as const)
    it(`a BroadcastChannel's ${kind} listeners of one publish share its context: what one writes the next one reads, and the publisher and the next publish don't`, async () => {
      const key = `context:${crypto.randomUUID()}`
      const MARK = Symbol('mark')
      const readByFirst: unknown[] = []
      const readBySecond: unknown[] = []
      const channel = new ServerBroadcast<string>({ key })
      const subscribe = (listener: () => void) =>
        kind === 'text' ? channel.subscribe(listener) : channel.subscribeBinary(listener)
      const first = subscribe(() => {
        const raw = getRawContext()
        readByFirst.push(raw?.[MARK])
        if (raw) raw[MARK] = `written ${readByFirst.length}`
      })
      const second = subscribe(() => readBySecond.push(getRawContext()?.[MARK]))
      await Promise.all([first.opened, second.opened])
      const readByPublisher = await asUser('bob', async () => {
        for (const n of [1, 2])
          if (kind === 'text') await Broadcast.publish(key, `publish ${n}`)
          else await Broadcast.publishBinary(key, new Uint8Array([n]))
        return getRawContext()?.[MARK]
      })
      expect({ readByFirst, readBySecond, readByPublisher }).toEqual({
        readByFirst: [undefined, undefined],
        readBySecond: ['written 1', 'written 2'],
        readByPublisher: undefined,
      })
      first()
      second()
    })
})
