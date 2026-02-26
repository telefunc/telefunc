export { handleError }

import type { ViteDevServer } from 'vite'
import { hasProp } from '../../../utils/hasProp.js'
import { getViteDevServer } from '../globalContext.js'

function handleError(err: unknown) {
  // Client disconnected mid-stream — not a bug, nothing to log.
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
