export { readTextResponse }

import { abortable, throwAbortError } from '../../../../client/remoteTelefunctionCall/errors.js'

const typedArrayPrototype = Object.getPrototypeOf(Uint8Array.prototype)

/** A text body, read as `response.text()` reads it, but that the call's abort cancels: an app's `config.fetch` may
 *  return a body the call's signal doesn't end. */
async function readTextResponse(
  response: Response,
  callContext: { abortController: AbortController; telefunctionName: string; telefuncFilePath: string },
): Promise<string> {
  const { body } = response
  // `whatwg-fetch` and other Fetch polyfills give no body stream to cancel: an abort ends the wait for their `.text()`.
  if (!body) return abortable(callContext, () => response.text())
  // A reader checks only the lock, where `.text()` also refuses a body read before.
  if (response.bodyUsed) throw new TypeError('Body has already been used')
  const { signal } = callContext.abortController
  const reader = body.getReader()
  const cancel = () => {
    reader.cancel().catch(() => {})
  }
  signal.addEventListener('abort', cancel, { once: true })
  if (signal.aborted) cancel()
  const decoder = new TextDecoder()
  let text = ''
  try {
    for (;;) {
      const { done, value } = await reader.read()
      if (signal.aborted) break
      if (done) return text + decoder.decode()
      // The intrinsic tag getter checks the type of the array across realms.
      if (Reflect.get(typedArrayPrototype, Symbol.toStringTag, value) !== 'Uint8Array') {
        throw new TypeError('Response body chunks must be Uint8Array')
      }
      const { buffer } = value
      if ((buffer as ArrayBuffer).detached) throw new TypeError('Response body chunk buffer is detached')
      // TextDecoder in browsers requires a non-shared buffer.
      const bytes =
        !(buffer instanceof ArrayBuffer) && Object.prototype.toString.call(buffer) === '[object SharedArrayBuffer]'
          ? new Uint8Array(value)
          : value
      text += decoder.decode(bytes, { stream: true })
    }
  } catch (err) {
    // Fetch can error the body on abort. For other failures, attempt cancellation and keep the read error.
    if (!signal.aborted) {
      cancel()
      throw err
    }
  } finally {
    signal.removeEventListener('abort', cancel)
    reader.releaseLock()
  }
  // The read ends early only on the call's abort.
  throwAbortError(callContext.telefunctionName, callContext.telefuncFilePath, undefined)
}
