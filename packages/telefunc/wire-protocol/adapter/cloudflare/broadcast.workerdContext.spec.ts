// spellcheck-ignore:file  nodejs_als is Cloudflare's AsyncLocalStorage compatibility flag
// A Worker with `nodejs_als` or `nodejs_compat`: workerd's AsyncLocalStorage, whose enterWith() throws.

import { AsyncLocalStorage } from 'node:async_hooks'
import { expect, it } from 'vitest'
import { installAsyncContext } from '../../../node/server/context/async.js'
import { restoreContext } from '../../../node/server/context/context.js'
import { PROVIDED_CONTEXT, getContext, provideTelefuncContext } from '../../../node/server/context/getContext.js'
import { CloudflareBroadcastTransport } from './broadcast.js'

// Node's own run() goes through enterWith(): it enters the store itself, so that only the app's call throws.
class WorkerdAsyncLocalStorage<T> extends AsyncLocalStorage<T | undefined> {
  override run<R, Args extends unknown[]>(store: T, fn: (...args: Args) => R, ...args: Args): R {
    const prior = this.getStore()
    super.enterWith(store)
    try {
      return fn(...args)
    } finally {
      super.enterWith(prior)
    }
  }
  override enterWith(): void {
    throw new Error('asyncLocalStorage.enterWith() is not implemented')
  }
}
installAsyncContext(WorkerdAsyncLocalStorage)

it("provideTelefuncContext() in a Durable Object's call is read after an await, and the call stays the object's", async () => {
  const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 5 })
  await transport.runInObject('telefunc-shard-weur-1', () =>
    restoreContext({ [PROVIDED_CONTEXT]: { user: 'grace' } }, async () => {
      provideTelefuncContext({ user: 'ada' })
      await new Promise((resolve) => setTimeout(resolve, 1))
      expect(getContext<{ user: string }>().user).toBe('ada')
      expect(transport.ownerOf(undefined)).toBe('telefunc-shard-weur-1')
    }),
  )
})

it('provideTelefuncContext() outside any scope is read until the next timer, as without AsyncLocalStorage', async () => {
  provideTelefuncContext({ user: 'ada' })
  await Promise.resolve()
  expect(getContext<{ user: string }>().user).toBe('ada')
  await new Promise((resolve) => setTimeout(resolve, 1))
  expect(() => getContext()).toThrow('provideTelefuncContext()')
})
