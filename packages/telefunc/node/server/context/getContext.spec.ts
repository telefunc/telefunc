// getContext() once AsyncLocalStorage is installed, inside an adapter's scope (telefunc/cloudflare's running object).

import { AsyncLocalStorage } from 'node:async_hooks'
import { expect, test, vi } from 'vitest'
import { installAsyncContext } from './async.js'
import { serve } from '../telefunc.js'
import { getRawContext, restoreContext } from './context.js'
import { PROVIDED_CONTEXT, getContext, provideTelefuncContext } from './getContext.js'

installAsyncContext(AsyncLocalStorage)
const RUNNING_OBJECT = Symbol.for('telefunc.cloudflare.object')

// The telefunctions below are this app's only file.
const telefunctions = vi.hoisted(() => ({}) as Record<string, () => Promise<unknown>>)
vi.mock('../runTelefunc/loadTelefuncFiles.js', () => ({
  loadTelefuncFiles: async () => ({
    telefuncFilesLoaded: { '/getContext.telefunc.ts': telefunctions },
    telefuncFilesAll: ['/getContext.telefunc.ts'],
  }),
}))

test("getContext() throws in an adapter's scope that holds none of the app's context", () => {
  restoreContext({ [RUNNING_OBJECT]: 'telefunc-shard-weur-0' }, () => {
    expect(() => getContext()).toThrow('Cannot access context object')
  })
})

test("a telefunction's context, inside an adapter's scope, is read after an await, beside the adapter's key", async () => {
  await restoreContext({ [RUNNING_OBJECT]: 'telefunc-shard-weur-0' }, () =>
    restoreContext({ [PROVIDED_CONTEXT]: { user: 'ada' } }, async () => {
      await new Promise((resolve) => setTimeout(resolve, 1))
      expect(getContext<{ user: string }>().user).toBe('ada')
    }),
  )
})

test('a provided context is read after an await, and one provided later leaves the code already running as it was', async () => {
  provideTelefuncContext({ user: 'ada' })
  const running = (async () => {
    await new Promise((resolve) => setTimeout(resolve, 1))
    return getContext<{ user: string }>().user
  })()
  provideTelefuncContext({ user: 'grace' })
  expect(await running).toBe('ada')
  expect(getContext<{ user: string }>().user).toBe('grace')
})

test("a telefunction served in an adapter's scope reads the adapter's key after an await", async () => {
  let read: unknown
  async function readRunningObject() {
    await new Promise((resolve) => setTimeout(resolve, 1))
    read = getRawContext()?.[RUNNING_OBJECT]
  }
  telefunctions.readRunningObject = readRunningObject
  const request = new Request('https://example.test/_telefunc', {
    method: 'POST',
    headers: { 'x-telefunc-request': 'txt' },
    body: JSON.stringify({ file: '/getContext.telefunc.ts', name: 'readRunningObject', args: [] }),
  })
  const response = await restoreContext({ [RUNNING_OBJECT]: 'telefunc-shard-weur-0' }, () => serve({ request }))
  expect(response.statusCode).toBe(200)
  expect(read).toBe('telefunc-shard-weur-0')
})
