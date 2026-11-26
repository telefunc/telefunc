export { handleError, isPeerDisconnect }

import type { ViteDevServer } from 'vite'
import { hasProp } from '../../../utils/hasProp.js'
import { getViteDevServer } from '../globalContext.js'
import { StreamTruncatedError } from '../../../wire-protocol/wire/BodyReader.js'
import { isNetworkError } from '../../../shared/NetworkError.js'

function handleError(err: unknown) {
  // The peer left: mid-upload, or mid-channel. Not bugs, nothing to log. `ERR_STREAM_PREMATURE_CLOSE`
  // is Node's own error, so it's the one that can only be duck-typed.
  if (isPeerDisconnect(err)) return
  if (hasProp(err, 'code') && err.code === 'ERR_STREAM_PREMATURE_CLOSE') return

  // We ensure we print a string; Cloudflare Workers doesn't seem to properly stringify `Error` objects.
  const errStr = (hasProp(err, 'stack') && String(err.stack)) || String(err)

  const viteDevServer = getViteDevServer()
  if (viteAlreadyLoggedError(err, viteDevServer)) {
    return
  }
  viteErrorCleanup(err, viteDevServer)

  console.error(errStr)
}

/** Telefunc's own errors for a peer that left. A server's own Node stream can also fail with `ERR_STREAM_PREMATURE_CLOSE`. */
function isPeerDisconnect(err: unknown): boolean {
  return err instanceof StreamTruncatedError || (isNetworkError(err) && err.isChannel)
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
