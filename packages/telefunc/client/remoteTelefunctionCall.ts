export { remoteTelefunctionCall }

import { makeHttpRequest } from './remoteTelefunctionCall/makeHttpRequest.js'
import { serializeTelefunctionArguments } from './remoteTelefunctionCall/serializeTelefunctionArguments.js'
import { resolveClientConfig } from './clientConfig.js'
import { assertUsage } from '../utils/assert.js'
import { isBrowser } from '../utils/isBrowser.js'
import { objectAssign } from '../utils/objectAssign.js'
import { isAsyncGenerator } from '../utils/isAsyncGenerator.js'
import { setAbortController } from './abort.js'
import { getPendingContext } from './withContext.js'

function remoteTelefunctionCall(
  telefuncFilePath: string,
  telefunctionName: string,
  telefunctionArgs: unknown[],
): Promise<unknown> {
  assertUsage(isBrowser(), 'The Telefunc Client is meant to be run only in the browser.')

  // Read pending context synchronously — set by withContext(), reset by its finally block.
  const callClientContext = getPendingContext()

  const callContext = {}

  {
    objectAssign(callContext, {
      telefuncFilePath,
      telefunctionName,
      telefunctionArgs,
    })
  }

  objectAssign(callContext, resolveClientConfig())

  if (callClientContext?.headers) {
    const merged = { ...callContext.headers, ...callClientContext.headers }
    objectAssign(callContext, { headers: merged })
  }

  const abortController = createAbortController(callClientContext?.signal)

  objectAssign(callContext, { abortController })

  {
    const httpRequestBody = serializeTelefunctionArguments(callContext)
    objectAssign(callContext, { httpRequestBody })
  }

  const telefunctionReturnPromise = makeHttpRequest(callContext)

  setAbortController(telefunctionReturnPromise, abortController)
  addAsyncGeneratorInterface(telefunctionReturnPromise, abortController)

  return telefunctionReturnPromise
}

/** Create an AbortController optionally wired to an external signal. */
function createAbortController(signal?: AbortSignal): AbortController {
  const abortController = new AbortController()

  if (signal) {
    if (signal.aborted) {
      abortController.abort()
    } else {
      signal.addEventListener('abort', () => abortController.abort(), { once: true })
    }
  }

  return abortController
}

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
