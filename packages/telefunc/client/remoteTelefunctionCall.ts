export { remoteTelefunctionCall }

import { makeHttpRequest } from './remoteTelefunctionCall/makeHttpRequest.js'
import { serializeTelefunctionArguments } from './remoteTelefunctionCall/serializeTelefunctionArguments.js'
import { resolveClientConfig } from './clientConfig.js'
import { assertUsage } from '../utils/assert.js'
import { isBrowser } from '../utils/isBrowser.js'
import { objectAssign } from '../utils/objectAssign.js'
import { setAbortController } from './abort.js'
import type { ClientCallContext } from './withContext.js'
import { addAsyncGeneratorInterface } from './remoteTelefunctionCall/async-generator-interface.js'

function remoteTelefunctionCall(
  telefuncFilePath: string,
  telefunctionName: string,
  telefunctionArgs: unknown[],
  callClientContext?: ClientCallContext,
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

  if (callClientContext?.headers) {
    const merged = { ...callContext.headers, ...callClientContext.headers }
    objectAssign(callContext, { headers: merged })
  }

  // Per-call transport overrides global config.transport
  if (callClientContext?.transport) {
    objectAssign(callContext, { transport: callClientContext.transport })
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
