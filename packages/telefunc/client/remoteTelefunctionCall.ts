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

  // Make the promise also async-iterable so that:
  //   for await (const x of onMyTelefunc()) { ... }
  // works without needing to first `await` the promise.
  // This makes the RPC boundary transparent for async generators.
  Object.assign(promise, {
    async *[Symbol.asyncIterator]() {
      const { telefunctionReturn } = await httpRequestPromise
      assertUsage(
        isAsyncGenerator(telefunctionReturn),
        '`for await...of` can only be used with telefunctions that return an async generator',
      )
      yield* telefunctionReturn as AsyncIterable<unknown>
    },
  })

  return promise
}
