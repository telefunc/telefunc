// Workaround: crossws's Node adapter gives `open` a peer whose `request` is another `Request` than the one its
// `upgrade` hook got, built anew from `Host`. Without the upgrade's own object carried in the peer's context, `open`
// finds nothing of what the upgrade hook decided by that request, and every Node WebSocket ends as it opens.
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
