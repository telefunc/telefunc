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

  const httpRequestPromise = makeHttpRequest(callContext)

  const promise = (async () => {
    const { telefunctionReturn } = await httpRequestPromise
    return telefunctionReturn
  })()

  return addAsyncGeneratorInterface(promise, httpRequestPromise)
}

/** Augment a promise with the AsyncGenerator interface
 *  so `for await...of` works directly without an intermediate `await`. */
function addAsyncGeneratorInterface(
  promise: Promise<unknown>,
  httpRequestPromise: Promise<{ telefunctionReturn: unknown }>,
): AsyncGenerator<unknown> & Promise<unknown> {
  let innerGen: AsyncGenerator<unknown> | null = null
  const getInnerGen = () =>
    (innerGen ??= (async function* () {
      const { telefunctionReturn } = await httpRequestPromise
      assertUsage(
        isAsyncGenerator(telefunctionReturn),
        '`for await...of` can only be used with telefunctions that return an async generator',
      )
      yield* telefunctionReturn as AsyncIterable<unknown>
    })())

  const augmented = Object.assign(promise, {
    next: (...args: [] | [unknown]) => getInnerGen().next(...args),
    return: (value?: unknown) => getInnerGen().return(value),
    throw: (e?: any) => getInnerGen().throw(e),
    [Symbol.asyncDispose]: async () => {
      await getInnerGen().return(undefined)
    },
    [Symbol.asyncIterator]: () => augmented,
  })
  return augmented
}
