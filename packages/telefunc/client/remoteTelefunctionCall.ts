export { remoteTelefunctionCall }

import { makeHttpRequest } from './remoteTelefunctionCall/makeHttpRequest.js'
import { serializeTelefunctionArguments } from './remoteTelefunctionCall/serializeTelefunctionArguments.js'
import { resolveClientConfig } from './clientConfig.js'
import { assertUsage } from '../utils/assert.js'
import { isBrowser } from '../utils/isBrowser.js'
import { objectAssign } from '../utils/objectAssign.js'
import { isAsyncGenerator } from '../utils/isAsyncGenerator.js'

function remoteTelefunctionCall(
  telefuncFilePath: string,
  telefunctionName: string,
  telefunctionArgs: unknown[],
): Promise<unknown> {
  assertUsage(isBrowser(), 'The Telefunc Client is meant to be run only in the browser.')

  const callContext = {}
  {
    objectAssign(callContext, {
      telefuncFilePath,
      telefunctionName,
      telefunctionArgs,
    })
  }

  objectAssign(callContext, resolveClientConfig())

  {
    const httpRequestBody = serializeTelefunctionArguments(callContext)
    objectAssign(callContext, { httpRequestBody })
  }

  const httpResponsePromise = makeHttpRequest(callContext)

  const telefunctionReturnPromise: Promise<unknown> = (async () => {
    const { telefunctionReturn } = await httpResponsePromise
    return telefunctionReturn
  })()

  return addAsyncGeneratorInterface(telefunctionReturnPromise)
}

/** Augment a promise with the AsyncGenerator interface
 *  so `for await...of` works directly without an intermediate `await`. */
function addAsyncGeneratorInterface(promise: Promise<unknown>): AsyncGenerator<unknown> & Promise<unknown> {
  let gen: AsyncGenerator<unknown> | null = null
  const getGen = () =>
    (gen ??= (async function* () {
      const returnValue = await promise
      assertUsage(
        isAsyncGenerator(returnValue),
        '`for await...of` can only be used with telefunctions that return an async generator',
      )
      yield* returnValue as AsyncIterable<unknown>
    })())

  const augmented = Object.assign(promise, {
    next: (...args: [] | [unknown]) => getGen().next(...args),
    return: (value?: unknown) => getGen().return(value),
    throw: (e?: any) => getGen().throw(e),
    [Symbol.asyncDispose]: async () => {
      await getGen().return(undefined)
    },
    [Symbol.asyncIterator]: () => augmented,
  })
  return augmented
}
