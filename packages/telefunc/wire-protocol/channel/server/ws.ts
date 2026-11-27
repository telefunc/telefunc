export { getTelefuncChannelOptions }

import { defineHooks, type Hooks, type Message, type Peer } from 'crossws'
import { upgradesTakenHere } from '../../../node/server/runTelefunc/takesUpgrades.js'
import { assert } from '../../../utils/assert.js'
import { readRequestUrl } from '../../../utils/getUrlPathname.js'
import { CHANNEL_TRANSPORT, WS_CLOSE_PAGE_LEFT, WS_CLOSE_REFUSED, WS_CLOSE_WIRE_VERSION } from '../../constants.js'
import { wireEnd } from '../../refusal.js'
import { namesWireVersion } from '../../wire/requestKind.js'
import { getChannelMux } from './ChannelMux.js'
import type { ServerTransport } from './ChannelMux.js'
import { upgradeRefusal } from './upgradeOrigin.js'
import { guardCrosswsHook, getCrosswsNamespace } from '../../../workarounds/crosswsHooks.js'

/** crossws's options for Telefunc's channels. `bufferedAmount` reads the socket's: `undefined` on workerd, whose
 *  WebSocket has none. `wireRequest` is the request the upgrade hook got, which crossws's peer carries as `peer.request`
 *  on every runtime but Node. */
function getTelefuncChannelOptions({
  upgrade,
  send = (peer, frame) => peer.send(frame),
  terminate = (peer) => peer.terminate(),
  bufferedAmount = (peer) => peer.websocket.bufferedAmount,
  wireRequest = (peer) => peer.request,
  copiesOnSend,
}: {
  upgrade?: Hooks['upgrade']
  send?: (peer: Peer, frame: Uint8Array<ArrayBuffer>) => void
  terminate?: (peer: Peer) => void
  bufferedAmount?: (peer: Peer) => number | undefined
  wireRequest?: (peer: Peer) => Request | undefined
  copiesOnSend?: boolean
} = {}) {
  const transport: ServerTransport<Peer> = {
    /** Persistent bidirectional WebSocket: no out-of-band POSTs to route, so no need for a
     *  connId-based reverse lookup. */
    getConnId: () => null,
    sendNow: send,
    type: CHANNEL_TRANSPORT.WS,
    wholeMessages: true,
    copiesOnSend,
    bufferedAmount,
    // A loss is terminated: the mux detaches the wire as it decides the loss, while a Durable Object's or Deno's
    // terminate() only starts a close handshake, which a vanished client never answers.
    // An end the server decided closes with its refusal as the reason, after what the socket already holds and ahead of
    // what the wire's sender holds back, which goes with the wire.
    close: (peer, { refusal }) => {
      if (refusal === null) terminate(peer)
      else peer.close(WS_CLOSE_REFUSED, wireEnd(refusal))
    },
  }
  /** Whether each handshake the upgrade hook let through names this server's wire version, by its request. */
  const sameVersion = new WeakMap<Request, boolean>()

  // A hook's throw `ends` its peer where it isn't ending already: the mux's peer goes as a loss, through the mux's one
  // close, so that it can't cut an end already going. The upgrade hook has no guard: neither `readableHost()` nor
  // `upgradeRefusal()` throws, and the adapters' own `upgrade` builds the peer's context.
  // The mux per call: a Cloudflare session DO hosts its own channels, and these hooks serve every one in the isolate.
  const hooks = defineHooks({
    // The one place that reads a handshake: `open` acts on what it decided.
    upgrade: (request) => {
      const host = readableHost(request)
      if (host === null) return new Response(null, { status: 400 })
      const refusal = upgradeRefusal(request, host)
      if (refusal) return refusal
      sameVersion.set(request, namesWireVersion(request))
      return upgrade?.(request)
    },
    // A throw in `open` comes before the mux has the peer, where nothing else would close it.
    open: guardCrosswsHook((peer) => {
      const request = wireRequest(peer)
      const named = request && sameVersion.get(request)
      assert(request && named !== undefined)
      // A page on another version reloads: none of its frames could be read. Told after the 101, as a close code: a page
      // learns nothing from a refused handshake.
      if (!named) return peer.close(WS_CLOSE_WIRE_VERSION, 'reload the page')
      getChannelMux().onWireOpen(peer, transport, upgradesTakenHere)
    }, terminate),
    message: guardCrosswsHook(
      // crossws types a message's bytes over any buffer; a WebSocket message's are never a SharedArrayBuffer's.
      (peer, message: Message) => getChannelMux().onWireMessage(peer, message.uint8Array() as Uint8Array<ArrayBuffer>),
      (peer) => getChannelMux().endFault(peer, { scope: 'loss' }),
    ),
    // Where the server decided the close, the mux forgot the peer as it decided it, and finds nothing here.
    close: guardCrosswsHook(
      (peer, details: Parameters<Hooks['close']>[1]) =>
        getChannelMux().onWireClosed(peer, details?.code === WS_CLOSE_PAGE_LEFT ? 'pageLeft' : 'transient'),
      null,
    ),
    error: guardCrosswsHook((peer) => getChannelMux().onWireClosed(peer, 'transient'), null),
  })
  return {
    hooks,
    getNamespace: getCrosswsNamespace,
  }
}

/** The `Host` of a handshake Telefunc can read: its URL is one, and its `Host` names an authority (a host, and a port
 *  where it has one) and nothing else, as a WebSocket handshake's must (RFC 6455, 4.2.1). Null otherwise, which is
 *  answered 400 (4.2.2). Never throws. */
function readableHost(request: Request): string | null {
  const host = request.headers.get('host')
  const readable =
    readRequestUrl(request.url) !== null && host !== null && !/[/\\?#@]/.test(host) && URL.canParse(`http://${host}`)
  return readable ? host : null
}
