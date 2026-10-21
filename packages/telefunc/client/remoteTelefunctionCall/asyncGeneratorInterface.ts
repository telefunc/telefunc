export { addAsyncGeneratorInterface }

import { assertUsage } from '../../utils/assert.js'
import { isAsyncGenerator } from '../../utils/isAsyncGenerator.js'
import { objectAssign } from '../../utils/objectAssign.js'
import { setAbortController } from '../abort.js'
import { setCloseHandlers } from '../close.js'

/** Augment a promise with the AsyncGenerator interface
 *  so `for await...of` works directly without an intermediate `await`. */
function addAsyncGeneratorInterface(promise: Promise<unknown>, abortController: AbortController) {
  // Single execution path: resolves to the real generator once the HTTP response is parsed.
  const resolvedGen: Promise<AsyncGenerator<unknown>> = (async () => {
    const returnValue = await promise
    assertUsage(isAsyncGenerator(returnValue), '`for await...of` can only be used for an async generator telefunction')
    setAbortController(returnValue, abortController)
    return returnValue
  })()

  // Register close handler synchronously so close(promise) works immediately,
  // even before the HTTP response arrives. The handler delegates to the real
  // generator once resolved, triggering cancelIndex on the demuxer.
  // resolvedGen.catch: suppresses unhandledRejection on the monitoring branch only —
  // the rejection still propagates through every awaited path (.next, .return, .throw).
  resolvedGen.catch(() => {})
  const closeHandlers = new WeakMap<object, () => void>()
  closeHandlers.set(promise as object, () => {
    void resolvedGen.then((g) => g.return(undefined))
  })
  setCloseHandlers(promise, closeHandlers)

  // Thin iteration shell — only used by .next(). Termination methods bypass
  // this and go directly to resolvedGen so cancelIndex always fires.
  const iter = (async function* () {
    yield* await resolvedGen
  })()

  objectAssign(promise, {
    next: (...args: [] | [unknown]) => iter.next(...args),
    return: async (value?: unknown) => (await resolvedGen).return(value),
    throw: async (e?: unknown) => (await resolvedGen).throw(e),
    [Symbol.asyncDispose]: async () => {
      await (await resolvedGen).return(undefined)
    },
    [Symbol.asyncIterator]: () => promise,
  })
}
