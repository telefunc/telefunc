export { addAsyncGeneratorInterface }

import { assertUsage } from '../../utils/assert.js'
import { isAsyncGenerator } from '../../utils/isAsyncGenerator.js'
import { objectAssign } from '../../utils/objectAssign.js'
import { setAbortController } from '../abort.js'

/** Augment a promise with the AsyncGenerator interface
 *  so `for await...of` works directly without an intermediate `await`. */
function addAsyncGeneratorInterface(promise: Promise<unknown>, abortController: AbortController) {
  let gen: AsyncGenerator<unknown> | null = null
  const getGen = () =>
    (gen ??= (async function* () {
      const returnValue = await promise
      assertUsage(
        isAsyncGenerator(returnValue),
        '`for await...of` can only be used with telefunctions that return an async generator',
      )
      setAbortController(returnValue, abortController)
      gen = returnValue
      yield* returnValue
    })())

  objectAssign(promise, {
    next: (...args: [] | [unknown]) => getGen().next(...args),
    return: (value?: unknown) => (gen ? gen.return(value) : Promise.resolve({ done: true as const, value })),
    throw: (e?: any) => (gen ? gen.throw(e) : Promise.reject(e)),
    [Symbol.asyncDispose]: () => (gen ? gen[Symbol.asyncDispose]() : Promise.resolve()),
    [Symbol.asyncIterator]: () => promise,
  })
}
