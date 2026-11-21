export { handleError }
export { logError }
export { describeValue }

import type { ViteDevServer } from 'vite'
import { hasProp } from '../../../utils/hasProp.js'
import { getViteDevServer } from '../globalContext.js'
import { StreamTruncatedError } from '../../../wire-protocol/wire/BodyReader.js'
import { isNetworkError } from '../../../shared/NetworkError.js'

function handleError(err: unknown) {
  // The peer left: mid-upload, mid-channel, or before the response. Not bugs, nothing to log. Node's own errors
  // (`ERR_STREAM_PREMATURE_CLOSE`, and `ERR_STREAM_UNABLE_TO_PIPE` for a streamed body) can only be duck-typed.
  if (err instanceof StreamTruncatedError) return
  if (isNetworkError(err) && err.isChannel) return
  if (hasProp(err, 'code') && (err.code === 'ERR_STREAM_PREMATURE_CLOSE' || err.code === 'ERR_STREAM_UNABLE_TO_PIPE'))
    return
  logError(err)
}

/** Logs `err` with its causes, where Vite didn't log it already. */
function logError(err: unknown) {
  // We ensure we print a string; Cloudflare Workers doesn't seem to properly stringify `Error` objects.
  const errStr = describeError(err)

  const viteDevServer = getViteDevServer()
  if (viteAlreadyLoggedError(err, viteDevServer)) {
    return
  }
  viteErrorCleanup(err, viteDevServer)

  console.error(errStr)
}

function viteAlreadyLoggedError(err: unknown, viteDevServer: ViteDevServer | null) {
  if (!viteDevServer) {
    return false
  }
  return viteDevServer.config.logger.hasErrorLogged(err as Error)
}

function viteErrorCleanup(err: unknown, viteDevServer: ViteDevServer | null) {
  if (!viteDevServer) {
    return false
  }
  if (hasProp(err, 'stack')) {
    // Apply source maps
    viteDevServer.ssrFixStacktrace(err as Error)
  }
}

/** Its stack, then each of its causes', as Node.js prints an error: a cause often says why, such as the broker's refusal
 *  under a backend subscription that closed. */
function describeError(err: unknown): string {
  const texts: string[] = []
  const seen = new Set<unknown>()
  let error = err
  while (error !== undefined && !seen.has(error)) {
    seen.add(error)
    texts.push((hasProp(error, 'stack') && String(error.stack)) || describeValue(error))
    error = hasProp(error, 'cause') ? error.cause : undefined
  }
  return texts.join('\n[cause]: ')
}

/** A value thrown or refused with, which may be no `Error`: a plain object is shown as JSON, and one `String()` throws on
 *  (an object without a prototype) too. */
function describeValue(value: unknown): string {
  let text: string
  try {
    text = String(value)
  } catch {
    return toJson(value) ?? Object.prototype.toString.call(value)
  }
  return text === '[object Object]' ? (toJson(value) ?? text) : text
}

function toJson(value: unknown): string | undefined {
  try {
    return JSON.stringify(value)
  } catch {
    return undefined
  }
}
