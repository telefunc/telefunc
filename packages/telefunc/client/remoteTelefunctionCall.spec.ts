import { afterEach, expect, it, vi } from 'vitest'
import { isAbort } from '../shared/Abort.js'
import { addTelefunctionCallBarrier, waitForTelefunctionCallBarriers } from '../wire-protocol/client/call-barrier.js'
import type { TelefunctionCallBarrierScope } from '../wire-protocol/client/call-barrier.js'
import { remoteTelefunctionCall } from './remoteTelefunctionCall.js'
import { withContext } from './withContext.js'
import type { ClientCallContext } from './withContext.js'

const originalWindow = Object.getOwnPropertyDescriptor(globalThis, 'window')

afterEach(() => {
  if (originalWindow === undefined) Reflect.deleteProperty(globalThis, 'window')
  else Object.defineProperty(globalThis, 'window', originalWindow)
})

it.each<[string, TelefunctionCallBarrierScope, ClientCallContext]>([
  ['URL', { telefuncUrl: '/scope-a' }, { telefuncUrl: '/scope-b' }],
  [
    'connection key',
    { telefuncUrl: '/scope-a', connectionKey: 'channel-a' },
    { telefuncUrl: '/scope-a', channel: { connectionKey: 'channel-b' } },
  ],
])('does not hold an unrelated %s behind a pending channel control', async (_label, barrierScope, callContext) => {
  const control = deferred<void>()
  addTelefunctionCallBarrier(barrierScope, control.promise)
  const fetch = installPendingFetch()
  const abortController = new AbortController()
  const call = withContext(() => remoteTelefunctionCall('/scope.telefunc.ts', 'unrelated', []), {
    ...callContext,
    signal: abortController.signal,
  })()
  try {
    await vi.waitFor(() => expect(fetch).toHaveBeenCalledOnce())
  } finally {
    control.resolve()
    abortController.abort()
    await call.catch(() => {})
  }
})

it('preserves the rejection that caused a dependent call barrier to fail', async () => {
  const control = deferred<void>()
  const scope = { telefuncUrl: '/scope-a' }
  addTelefunctionCallBarrier(scope, control.promise)
  const barrier = waitForTelefunctionCallBarriers(scope)
  const failure = new Error('control rejected')
  control.reject(failure)

  await expect(barrier).rejects.toBe(failure)
})

it('lets abort win while a matching channel control remains pending', async () => {
  const control = deferred<void>()
  addTelefunctionCallBarrier({ telefuncUrl: '/scope-a' }, control.promise)
  const fetch = installPendingFetch()
  const abortController = new AbortController()
  const call = withContext(() => remoteTelefunctionCall('/scope.telefunc.ts', 'aborted', []), {
    telefuncUrl: '/scope-a',
    signal: abortController.signal,
  })()
  let failure: unknown
  void call.catch((error) => {
    failure = error
  })
  try {
    abortController.abort()
    await new Promise<void>((resolve) => setTimeout(resolve, 0))
    expect(isAbort(failure)).toBe(true)
    expect(fetch).not.toHaveBeenCalled()
  } finally {
    control.resolve()
    await call.catch(() => {})
  }
})

function installPendingFetch() {
  const fetch = vi.fn(
    (_input: RequestInfo | URL, init?: RequestInit) =>
      new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener('abort', () => reject(new Error('fetch aborted')), { once: true })
      }),
  )
  Object.defineProperty(globalThis, 'window', {
    configurable: true,
    value: { fetch, location: { href: 'http://localhost/' }, scrollY: 0 },
  })
  return fetch
}

function deferred<T>() {
  let resolve!: (value?: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise as (value?: T) => void
    reject = rejectPromise
  })
  void promise.catch(() => {})
  return { promise, resolve, reject }
}
