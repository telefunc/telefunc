import { AsyncLocalStorage } from 'node:async_hooks'
import { afterEach, beforeAll, describe, expect, it } from 'vitest'
import { Broadcast, ServerBroadcast } from './ServerBroadcast.js'
import { disposeBackend } from '../../backend/install.js'
import { getContext, PROVIDED_CONTEXT } from '../../../node/server/context/getContext.js'
import { restoreContext } from '../../../node/server/context/context.js'
import { installAsyncContext } from '../../../node/server/context/async.js'
import type { Unsubscribe } from '../channel.js'

afterEach(() => disposeBackend())

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
      await unsubscribe.ready
      await asUser('bob', () => publish(key))
      // Without AsyncLocalStorage, a context lasts until the next macrotask.
      await new Promise((resolve) => setTimeout(resolve, 0))
      await publish(key)
      expect(read).toEqual(['throws', 'throws'])
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
})
