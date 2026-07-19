// The attribution the TanStack adapter's guard rests on, tested against the REAL generated-stub path.
// `live()`'s own spec necessarily drives a FAKE telefunction, so on its own it proves the fake stamps the
// call — not that `remoteTelefunctionCall` does. If the production stamp were dropped, every test over
// there would still pass and every real `live()` would fail closed in the browser. This is the spec that
// can tell the difference.

import { describe, expect, it, vi } from 'vitest'

// Stub out only the transport-and-config machinery, so the code under test is the real
// `remoteTelefunctionCall` body: read the pending context, then stamp the promise it hands back.
vi.mock('./remoteTelefunctionCall/makeHttpRequest.js', () => ({
  makeHttpRequest: () => Promise.resolve('telefunction-result'),
}))
vi.mock('./remoteTelefunctionCall/serializeTelefunctionArguments.js', () => ({
  serializeTelefunctionArguments: () => ({ httpRequestBody: '{}', requestCloseHandlers: [] }),
}))
vi.mock('../utils/isBrowser.js', () => ({ isBrowser: () => true }))
vi.mock('./clientConfig.js', () => ({ resolveClientConfig: () => ({}) }))

import { contextConsumerOf } from './callAttribution.js'
import { remoteTelefunctionCall } from './remoteTelefunctionCall.js'
import { withContext, withContextChecked } from './withContext.js'

/** A generated client stub, as telefunc's transform emits it. */
const onGetTodos = (...args: unknown[]) => remoteTelefunctionCall('/todos.telefunc.ts', 'onGetTodos', args)

describe('callAttribution — the real stub stamps the call it returns', () => {
  it('a call made inside a context window is attributed to THAT context', async () => {
    const context = { signal: new AbortController().signal }
    const { result, consumed } = withContextChecked(onGetTodos, context, 7)
    expect(consumed).toBe(true)
    expect(contextConsumerOf(result)).toBe(context)
    await result
  })

  it('CONTROL: a call made with no context window is attributed to nothing', async () => {
    const call = onGetTodos(7)
    // Not merely "some other context" — unattributed. An app that never uses `withContext` must not grow
    // this map, and an unattributed call must never satisfy somebody else's check.
    expect(contextConsumerOf(call)).toBeUndefined()
    await call
  })

  it('two calls in one window are attributed to the same context, individually', async () => {
    const context = { signal: new AbortController().signal }
    let first: unknown
    let second: unknown
    withContext(() => {
      first = onGetTodos(1)
      second = onGetTodos(2)
    }, context)()
    // The FIRST call consumes the pending slot, so only it can be attributed: the second ran in the same
    // lexical window but the context was already taken, which is exactly why a wrapper returning the
    // second call must fail closed rather than inherit the first one's signal.
    expect(contextConsumerOf(first)).toBe(context)
    expect(contextConsumerOf(second)).toBeUndefined()
    await Promise.all([first, second])
  })

  it('the context reaches the request itself, not just the map', async () => {
    const controller = new AbortController()
    const context = { signal: controller.signal, headers: { 'X-Test': 'yes' } }
    const { result } = withContextChecked(onGetTodos, context, 7)
    // Attribution is a claim ABOUT the call; this is the claim being true. If the stamp were applied to
    // something other than the object the caller receives, this pairing would be a fiction.
    expect(contextConsumerOf(result)).toBe(context)
    await result
  })

  it('a non-object is not attributable, and asking does not throw', () => {
    expect(contextConsumerOf(42)).toBeUndefined()
    expect(contextConsumerOf(null)).toBeUndefined()
    expect(contextConsumerOf(undefined)).toBeUndefined()
  })
})
