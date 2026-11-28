// Workaround: crossws's Node adapter gives `open` a `NodeReqProxy` built from `http://${host}${req.url}` instead of
// reusing the `Request` passed to `handleUpgrade`, breaking absolute-form request targets when `Host` includes a port.
// TODO: remove once https://github.com/h3js/crossws/commit/442ba0b23a204e8ce8d1445f3fb3b5fcb9768561 is released and Telefunc updates crossws.
export { carryUpgradeRequest }

import type { Peer, PeerContext } from 'crossws'

declare module 'crossws' {
  interface PeerContext {
    /** The request the upgrade hook got. */
    request?: Request
  }
}

/** crossws's `upgrade` with the peer's `context` (`contextOf`) carrying the request the hook got, and `wireRequest`,
 *  which reads it back. */
function carryUpgradeRequest(contextOf: (request: Request) => PeerContext): {
  upgrade: (request: Request) => { context: PeerContext }
  wireRequest: (peer: Peer) => Request | undefined
} {
  return {
    upgrade: (request) => {
      const context = contextOf(request)
      context.request = request
      return { context }
    },
    wireRequest: (peer) => peer.context.request,
  }
}
