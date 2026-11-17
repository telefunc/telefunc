export { handleError }

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

  // We ensure we print a string; Cloudflare Workers doesn't seem to properly stringify `Error` objects.
  const errStr = (hasProp(err, 'stack') && String(err.stack)) || String(err)

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
